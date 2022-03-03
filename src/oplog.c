/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "bonsai.h"
#include "oplog.h"
#include "ordo.h"
#include "arch.h"
#include "cpu.h"
#include "region.h"
#include "common.h"
#include "thread.h"
#include "pnode.h"
#include "shim.h"
#include "hash.h"
#include "epoch.h"

extern struct bonsai_info *bonsai;

typedef struct {
	int cpu;
	struct log_page_desc* page;
	struct list_head list;
} flush_page_struct;

struct oplog_blk* alloc_oplog_block(int cpu) {
	struct log_layer* layer = LOG(bonsai);
	struct log_region* region = &layer->region[cpu];
	struct log_region_desc* desc = region->desc;
	struct log_page_desc* page;
	struct oplog_blk *new_block, *old_block;
	__le64 old_val, new_val;

again:
	old_val = desc->r_oplog_top; new_val = old_val + sizeof(struct oplog_blk);

	if (unlikely(!(new_val & ~PAGE_MASK) || !old_val)) {
		/* we reach a page end or it is first time to allocate an oplog block */
		page = alloc_log_page(region);
		new_val = LOG_REGION_ADDR_TO_OFF(region, page) + sizeof(struct oplog_blk);
	} else {
		old_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, old_val);
		page = LOG_PAGE_DESC(old_block);
	}

	if (cmpxchg(&desc->r_oplog_top, old_val, new_val) != old_val)
		goto again;

	new_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, new_val);
	new_block->cnt = 0;
	new_block->cpu = cpu;
	new_block->epoch = __this->t_epoch;
	new_block->next = 0;

	if (likely(old_val)) {
		old_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, old_val);
		old_block->next = LOG_REGION_ADDR_TO_OFF(region, new_block);
		bonsai_flush((void*)&old_block->next, sizeof(__le64), 0);
	}

	return new_block;
}

#define CHECK_NLOG_INTERVAL     20

struct oplog* alloc_oplog(struct log_region* region, int cpu) {
    static __thread int last = 0;
	volatile struct oplog_blk* block = region->curr_blk;
	struct log_layer* layer = LOG(bonsai);
	struct oplog* log;

	if (unlikely(!block || block->cnt == NUM_OPLOG_PER_BLK)) {
        try_run_epoch();

		block = alloc_oplog_block(cpu);

        /*
         * We issue a mfence here, to ensure the newly allocated
         * block is correctly inserted into log blk list globally.
         * The pflusher may walk thru that list after modifying
         * @region->curr_blk.
         */
        smp_mb();

		region->curr_blk = block;
	}

	log = &block->logs[block->cnt++];

    if (++last == CHECK_NLOG_INTERVAL) {
        if (atomic_add_return(CHECK_NLOG_INTERVAL, &layer->nlogs[cpu].cnt) >= CHKPT_NLOG_INTERVAL / NUM_CPU &&
            !atomic_read(&layer->checkpoint)) {
            wakeup_master();
        }
        last = 0;
    }

	return log;
}

struct oplog *oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, int cpu, struct pnode *pnode) {
	struct log_layer* layer = LOG(bonsai);
	unsigned int epoch;
	struct log_region *region;
	struct oplog_blk* block;
	struct oplog* log;

	region = &layer->region[cpu];

	log = alloc_oplog(region, cpu);
	
	block = OPLOG_BLK(log);
	epoch = block->epoch;

	log->o_type = cpu_to_le8(op);
	log->o_numa_node = cpu_to_le8(numa_node);
	log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_kv.k = key;
	log->o_kv.v = val;

    assert(epoch == ACCESS_ONCE(__this->t_epoch));

	if (unlikely(region->curr_blk->cnt == NUM_OPLOG_PER_BLK)) {
		/* persist it, no memory fence */
		bonsai_flush((void*)region->curr_blk, sizeof(struct oplog_blk), 0);
	}

	bonsai_debug("thread [%d] insert an oplog <%lu, %lu>\n", __this->t_id, key, val);

	return log;
}

static void try_free_log_page(struct oplog_blk* block, struct list_head* page_list) {
	struct log_page_desc *page;
	flush_page_struct *p;

	asm volatile("lock; decl %0" : "+m" (block->cnt));

	if (!ACCESS_ONCE(block->cnt)) {
		page = LOG_PAGE_DESC(block);

        /* We free the page if and only if we flushed its last block. */
        if ((void *) block + sizeof(*block) == (void*) page + PAGE_SIZE) {
			p = malloc(sizeof(flush_page_struct));
			p->cpu = block->cpu;
			p->page = page;
			INIT_LIST_HEAD(&p->list);
			list_add(&p->list, page_list);
        }
	}
}

void clean_pflush_buckets(struct log_layer* layer) {
	struct hbucket* bucket;
	struct hlist_node *hnode, *tmp;
	merge_ent* e = NULL;
	int i;
 
	for (i = 0; i < NUM_MERGE_HASH_BUCKET; i ++) {
		bucket = &layer->buckets[i];
		spin_lock(&bucket->lock);
		hlist_for_each_entry_safe(e, hnode, tmp, &bucket->head, node) {
			hlist_del(&e->node);
			free(e);
		}
		spin_unlock(&bucket->lock);
	}
}

/*
 * worker_scan_buckets: scan buckets and put entries into the list
 */
static void worker_scan_buckets(struct log_layer* layer) {
	int i, id = __this->t_id, num_bucket_per_thread = NUM_MERGE_HASH_BUCKET / (NUM_PFLUSH_THREAD - 1);
	int min_index = (id - 2) * num_bucket_per_thread;
	int max_index = (id - 1) * num_bucket_per_thread - 1;
	struct hbucket* bucket;
	struct hlist_node *hnode, *tmp;
	struct list_head* head = &layer->sort_list[id - 2];
	merge_ent* e;
	int count = 0;

	INIT_LIST_HEAD(head);

	bonsai_debug("pflush thread[%d] scan bucket [%u %u]\n", id, min_index, max_index);

	pthread_barrier_wait(&layer->barrier);
	
	for (i = min_index; i <= max_index; i ++) {
		bucket = &layer->buckets[i];
		
		spin_lock(&bucket->lock);
		hlist_for_each_entry_safe(e, hnode, tmp, &bucket->head, node) {
			hlist_del(&e->node);
			list_add(&e->list, head);
			count++;
		}
		spin_unlock(&bucket->lock);
	}
}

static int list_sort_cmp(void *priv, struct list_head *a, struct list_head *b) {
	merge_ent* x = list_entry(a, merge_ent, list);
	merge_ent* y = list_entry(b, merge_ent, list);

	return pkey_compare(x->log->o_kv.k, y->log->o_kv.k);
}

static void __list_sort(struct list_head* head) {
	list_sort(NULL, head, list_sort_cmp);
}

static void copy_list(struct list_head* dst, struct list_head* src) {
	merge_ent *e, *n;

	list_for_each_entry(n, src, list) {
		e = malloc(sizeof(merge_ent));
		*e = *n;
		
		list_add_tail(&e->list, dst);
	}
}

static void free_second_half_list(struct list_head* head) {
	struct list_head* pos;
	merge_ent *e, *tmp;
	int sum = 0, half, i = 0;

	list_for_each(pos, head) {
		sum ++;
	}

	half = sum / 2;
	if (sum % 2) half += 1;
	
	list_for_each_entry_safe_reverse(e, tmp, head, list) {
		if (i ++ < half) {
			list_del(&e->list);
			free(e);
		} else {
			break;
		}
	}
}

static void free_first_half_list(struct list_head* head) {
	struct list_head* pos;
	merge_ent *e, *tmp;
	int sum = 0, half, i = 0;

	list_for_each(pos, head) {
		sum ++;
	}

	half = sum / 2;
	list_for_each_entry_safe(e, tmp, head, list) {
		if (i ++ < half) {
			list_del(&e->list);
			free(e);
		} else {
			break;
		}
	}
}

/*
 * worker_sort_log: multi-threading parallel sorting
 */
static void worker_sort_log(struct log_layer* layer) {
	int i, N = NUM_PFLUSH_WORKER, id = __this->t_id - 2;

	bonsai_debug("pflush thread[%d] sort\n", __this->t_id);

	if (unlikely(NUM_PFLUSH_WORKER == 1)) {
		__list_sort(&layer->sort_list[id]);
		return;
	}
	
	pthread_barrier_wait(&layer->barrier);

	__list_sort(&layer->sort_list[id]);

	pthread_barrier_wait(&layer->barrier);

	for (i = 0; i < N; i ++) {
		if (i % 2 == 0) {
			if (id % 2 == 0) {
				list_splice(&layer->sort_list[id], &layer->sort_list[id + 1]);
				INIT_LIST_HEAD(&layer->sort_list[id]);
				copy_list(&layer->sort_list[id], &layer->sort_list[id + 1]);
				pthread_barrier_wait(&layer->barrier);
				
				__list_sort(&layer->sort_list[id]);
				free_second_half_list(&layer->sort_list[id]);
				pthread_barrier_wait(&layer->barrier);	
			} else {
				pthread_barrier_wait(&layer->barrier);
				__list_sort(&layer->sort_list[id]);
				free_first_half_list(&layer->sort_list[id]);
				pthread_barrier_wait(&layer->barrier);	
			}
		} else {
			if (id % 2 == 0) {
				pthread_barrier_wait(&layer->barrier);
				if (id != 0) {
					__list_sort(&layer->sort_list[id]);
					free_first_half_list(&layer->sort_list[id]);
				}
				pthread_barrier_wait(&layer->barrier);	
			} else {
				if (id != NUM_PFLUSH_WORKER - 1) {
					list_splice(&layer->sort_list[id], &layer->sort_list[id + 1]);
					INIT_LIST_HEAD(&layer->sort_list[id]);
					copy_list(&layer->sort_list[id], &layer->sort_list[id + 1]);
				}

				pthread_barrier_wait(&layer->barrier);
				if (id != NUM_PFLUSH_WORKER - 1) {
					__list_sort(&layer->sort_list[id]);
					free_second_half_list(&layer->sort_list[id]);
				}
				pthread_barrier_wait(&layer->barrier);	
			}
		}
	}
}

static int worker_oplog_merge_and_sort(void *arg) {
	struct merge_work* mwork = (struct merge_work*)arg;
	struct log_layer* layer;
	volatile struct oplog_blk* block;
	struct oplog *log;
	struct hbucket* bucket;
	struct hlist_node* hnode;
	struct log_region* region;
	merge_ent* e, *target;
	pkey_t key;
	int i, j, count = 0, ret = 0;
	int nlog = 0, nblk = 0;

	if (unlikely(!mwork))
		goto out;

	layer = mwork->layer;

	bonsai_print("pflush thread[%d] merge %d log lists\n", __this->t_id, mwork->count);

	/* 1. merge logs */
	for (i = 0; i < mwork->count; i ++) {
		block = mwork->first_blks[i];

        assert(block != mwork->last_blks[i]);

		do {
			count = block->cnt;
			
			for (j = 0; j < count; j ++) {
				log = &block->logs[j];
				key = log->o_kv.k;
				bucket = &layer->buckets[p_hash(key)];

				try_free_log_page((struct oplog_blk*)block, &mwork->page_list);

				nlog++;
                atomic_dec(&layer->nlogs[block->cpu].cnt);
				bonsai_debug("pflush thread[%d] merge <%lu, %lu> in bucket[%d]\n", 
						__this->t_id, log->o_kv.k, log->o_kv.v, p_hash(key));

                target = NULL;

				spin_lock(&bucket->lock);

                hlist_for_each_entry(e, hnode, &bucket->head, node) {
					if (!pkey_compare(e->log->o_kv.k, key)) {
                        target = e;
                        break;
					}
				}

                if (!target) {
				    target = malloc(sizeof(merge_ent));
                    INIT_HLIST_NODE(&target->node);
                    hlist_add_head(&target->node, &bucket->head);
                    target->log = log;
                } else if (ordo_cmp_clock(target->log->o_stamp, log->o_stamp) == ORDO_LESS_THAN) {
                    target->log = log;
                }

                /*
                 * target->log should be globally visible now,
                 * thanks to the implicit fence of spin_unlock.
                 */
				spin_unlock(&bucket->lock);
			}

			nblk ++;
			region = &layer->region[block->cpu];
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);

			if (unlikely(atomic_read(&layer->exit))) {
				free(mwork);
				ret = -EEXIT;
				goto out;
			}
			
		} while(block != mwork->last_blks[i]);
	}

out:
	/* 2. copy */
	worker_scan_buckets(layer);

	/* 3. sort */
	worker_sort_log(layer);

	bonsai_print("pflush thread[%d] merge %d logs %d blocks\n", __this->t_id, nlog, nblk);
	return ret;
}

#if 0
static int worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct log_layer* layer = fwork->layer;
	struct hbucket* bucket;
	struct hlist_node *hnode, *tmp;
	struct oplog* log;
	merge_ent* e;
	unsigned int i;
	int count = 0, ret = 0;

	bonsai_print("pflush thread[%d] flush bucket [%u %u]\n", __this->t_id, fwork->min_index, fwork->max_index);

	for (i = fwork->min_index; i <= fwork->max_index; i ++, fwork->curr_index ++) {
		bucket = &layer->buckets[i];
		hlist_for_each_entry_safe(e, hnode, tmp, &bucket->head, node) {
			log = e->log;
			bonsai_debug("pflush thread[%d] flush <%lu %lu>[%s] in bucket[%d]\n", __this->t_id, log->o_kv.k, log->o_kv.v, log->o_type ? "remove" : "insert", i);
			
			switch(log->o_type) {
			case OP_INSERT:
				pnode_insert(log->o_kv.k, log->o_kv.v, log->o_stamp, log->o_numa_node);		
				break;
			case OP_REMOVE:
				pnode_remove(log->o_kv.k);
				break;
			default:
				perror("bad operation type\n");
				break;
			}

			hlist_del(&e->node);
			free(e);

			count ++;
		}

		if (unlikely(atomic_read(&layer->exit))) {
			ret = -EEXIT;
			goto out;
		}
	}

out:
	free(fwork->small_free_set);
	free(fwork->big_free_set);
	free(fwork);
	bonsai_print("pflush thread[%d] flush %d logs\n", __this->t_id, count);

	return ret;
}
#endif

static int worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct log_layer* layer = fwork->layer;
	struct oplog* log;
	merge_ent* e, *tmp;
	int count = 0, ret = 0;

	bonsai_print("pflush thread[%d] start flush\n", __this->t_id);

	list_for_each_entry_safe(e, tmp, &fwork->flush_list, list) {
		log = e->log;
		
		bonsai_debug("pflush thread[%d] flush <%lu %lu> [%s]\n", 
			__this->t_id, log->o_kv.k, log->o_kv.v, log->o_type ? "remove" : "insert");

		switch(log->o_type) {
		case OP_INSERT:
            pnode_insert(log->o_kv.k, &log->o_kv.v);
			break;
		case OP_REMOVE:
			//pnode_remove(log->o_kv.k);
			break;
		default:
			perror("bad operation type\n");
			break;
		}

		list_del(&e->list);
		free(e);

		count ++;

		if (unlikely(count % 10 == 0 && atomic_read(&layer->exit))) {
			ret = -EEXIT;
			goto out;
		}
	}

out:
	free(fwork->small_free_set);
	free(fwork->big_free_set);
	free(fwork);
	bonsai_print("pflush thread[%d] flush %d logs\n", __this->t_id, count);

	return ret;
}

static void free_pages(struct log_layer *layer, struct list_head* page_list) {
	flush_page_struct *p, *n;
	//int i = 0;
	
	list_for_each_entry_safe(p, n, page_list, list) {
		free_log_page(&layer->region[p->cpu], p->page);
		bonsai_debug("free log page[%d]: addr: %016lx\n", 0, p->page);
		free(p);
	}
}

/*
 * oplog_flush: perform a full log flush
 */
void oplog_flush() {
	struct log_layer *l_layer = LOG(bonsai);
	struct data_layer *d_layer = DATA(bonsai);
	struct log_region *region;
	volatile struct oplog_blk* first_blks[NUM_CPU];
	volatile struct oplog_blk* curr_blks[NUM_CPU];
	volatile struct oplog_blk* curr_blk;
	struct merge_work* mwork;
	struct merge_work* mworks[NUM_PFLUSH_THREAD];
	struct flush_work* fwork;
	struct flush_work* fworks[NUM_PFLUSH_WORKER];
	struct work_struct* work;
	merge_ent *e, *tmp;
	struct pnode* pnode;
	int i, j, cpu, cnt = 0, total = 0;
	unsigned long n;
	int num_region_per_thread, num_region_rest;
	int num_block_per_thread;
    int nlogs;
    struct merge_work empty_mwork = { .layer = l_layer };
	//int num_bucket_per_thread;

	bonsai_print("thread[%d]: start oplog checkpoint [%d]\n", __this->t_id, l_layer->nflush);

	atomic_set(&l_layer->checkpoint, 1);

	/* 1. fetch and merge all log lists */
    nlogs = 0;
	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		region = &l_layer->region[cpu];
        nlogs += atomic_read(&l_layer->nlogs[cpu].cnt);
		curr_blk = ACCESS_ONCE(region->curr_blk);
        /* TODO: Non-full log blocks can not be flushed! */
		if (curr_blk && region->first_blk != curr_blk) {
			first_blks[total] = region->first_blk;
			curr_blks[total] = curr_blk;
			region->first_blk = curr_blk;
			total ++;
		}
	}

	if (unlikely(!total)) 
		goto out;

	if (unlikely(atomic_read(&l_layer->exit)))
		goto out;

	/* 2. merge all logs */
	if (total == 1) {
		/* case I: total == 1 */
		num_region_per_thread = 1;
		num_block_per_thread = nlogs / NUM_OPLOG_PER_BLK / NUM_PFLUSH_WORKER;
		curr_blk = first_blks[0];
		for (i = 1; i < NUM_PFLUSH_THREAD; i ++) {
			mwork = malloc(sizeof(struct merge_work));
			mwork->count = num_block_per_thread ? num_region_per_thread : 0;
			mwork->layer = l_layer;
			INIT_LIST_HEAD(&mwork->page_list);
			mwork->first_blks[0] = curr_blk;
			if (i != NUM_PFLUSH_THREAD - 1) {
				for (j = 0; j < num_block_per_thread; j ++)
					curr_blk = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR((&l_layer->region[curr_blk->cpu]), curr_blk->next);
			} else {
                curr_blk = curr_blks[0];
			}
			mwork->last_blks[0] = curr_blk;
			mworks[i] = mwork;
		}
	} else if (total < NUM_PFLUSH_WORKER) {
		/* case II: total < (NUM_PFLUSH_THREAD - 1) */
		num_region_per_thread = 1;
		for (i = 1, j = 0; i < (total + 1); i ++) {
			mwork = malloc(sizeof(struct merge_work));
			mwork->count = num_region_per_thread;
			mwork->layer = l_layer;
			INIT_LIST_HEAD(&mwork->page_list);
			while (j < num_region_per_thread) {
				mwork->first_blks[j] = first_blks[cnt];
				mwork->last_blks[j] = curr_blks[cnt];
				j ++; cnt ++;
			}
			j = 0;
			mworks[i] = mwork;
		}
		for (i = total + 1; i < NUM_PFLUSH_THREAD; i ++)
			mworks[i] = &empty_mwork;
	} else {
		/* case III: total >= NUM_PFLUSH_WORKER */
		num_region_per_thread = total / NUM_PFLUSH_WORKER;
		for (i = 1, j = 0; i < NUM_PFLUSH_WORKER; i ++) {
			mwork = malloc(sizeof(struct merge_work));
			mwork->count = num_region_per_thread;
			mwork->layer = l_layer;
			INIT_LIST_HEAD(&mwork->page_list);
			while (j < num_region_per_thread) {
				mwork->first_blks[j] = first_blks[cnt];
				mwork->last_blks[j] = curr_blks[cnt];
				j ++; cnt ++;
			}
			j = 0;
			mworks[i] = mwork;
		}
		j = 0;
		num_region_rest = total - (NUM_PFLUSH_WORKER - 1) * num_region_per_thread;
		mwork = malloc(sizeof(struct merge_work));
		mwork->count = num_region_rest;
		mwork->layer = l_layer;
		INIT_LIST_HEAD(&mwork->page_list);
		while (j < num_region_rest) {
			mwork->first_blks[j] = first_blks[cnt];
			mwork->last_blks[j] = curr_blks[cnt];
			j ++; cnt ++;
		}
		mworks[i] = mwork;
	}
	
	for (cnt = 1; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->exec = worker_oplog_merge_and_sort;
		work->exec_arg = (void*)(mworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);
	}

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 3. flush all logs */
	//num_bucket_per_thread = NUM_MERGE_HASH_BUCKET / (NUM_PFLUSH_THREAD - 1);
	for (cnt = 1; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		fwork = malloc(sizeof(struct flush_work));
		//fwork->min_index = (cnt - 1) * num_bucket_per_thread;
		//fwork->max_index = cnt * num_bucket_per_thread - 1;
		//fwork->curr_index = fwork->min_index;
		fwork->small_free_cnt = 0;
		fwork->big_free_cnt = 0;
		//fwork->small_free_set = malloc(sizeof(pkey_t) * SEGMENT_SIZE * LOAD_FACTOR_DEFAULT);
		//fwork->big_free_set = malloc(sizeof(pkey_t) * MAX_NUM_BUCKETS);
        /* TODO: What are these? */
        fwork->small_free_set = malloc(1);
        fwork->big_free_set = malloc(1);
		fwork->layer = l_layer;
		INIT_LIST_HEAD(&fwork->flush_list);

		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->exec = worker_oplog_flush;
		work->exec_arg = (void*)fwork;
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);

		fworks[cnt - 1] = fwork;
	}

	/* 4. split sort list */
	for (i = 0, n = 0; i < NUM_PFLUSH_WORKER; i ++) {
		list_for_each_entry_safe(e, tmp, &l_layer->sort_list[i], list) {
			list_del(&e->list);
			list_add_tail(&e->list, &fworks[n++ % NUM_PFLUSH_WORKER]->flush_list);
		}
	}

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 5. traverse the pnode list */
	//write_lock(&d_layer->lock);
	//list_for_each_entry(pnode, &d_layer->pnode_list, list)
	//	pnode->stale = PNODE_DATA_CLEAN;
	//write_unlock(&d_layer->lock);

	/* 6. free all pages */
	for (i = 1; i < NUM_PFLUSH_THREAD; i ++) {
        if (mworks[i] != &empty_mwork) {
            free_pages(l_layer, &mworks[i]->page_list);
            free(mworks[i]);
        }
	}

out:
	/* 7. finish */
	l_layer->nflush ++;
	atomic_set(&l_layer->checkpoint, 0);

	//dump_pnode_list_summary();
	bonsai_print("thread[%d]: finish log checkpoint [%d]\n", __this->t_id, l_layer->nflush);
}

static struct oplog* scan_one_cpu_log_region(struct log_region *region, pkey_t key) {
	volatile struct oplog_blk* block;
	struct oplog *log;
	int i;

	if (region->curr_blk) {
		block = region->first_blk;
		while (1) {
			for (i = 0; i < block->cnt; i ++) {
				log = &block->logs[i];
				if (!pkey_compare(log->o_kv.k, key)) {
					return log;
				}
			}
			if (block == region->curr_blk)
				return NULL;
			
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);
		}
	}

	return NULL;
}

/*
 * log_layer_search_key: scan @cpu log region and search @key,
 * find @cpu equals -1, scan all log regions, you must stop the world first.
 */
struct oplog* log_layer_search_key(int cpu, pkey_t key) {
	struct log_layer *l_layer = LOG(bonsai);
	struct log_region *region;
	struct oplog* log = NULL;

	if (cpu != -1) {
		region = &l_layer->region[cpu];
		log = scan_one_cpu_log_region(region, key);
	}
	else {
		for (cpu = 0; cpu < NUM_CPU; cpu ++) {
			region = &l_layer->region[cpu];
			log = scan_one_cpu_log_region(region, key);
			if (log) 
				goto out;
		}
	}

out:
	bonsai_print("log layer search key[%lu]: log %016lx\n", log ? log->o_kv.k : key, log);
	return log;
}
