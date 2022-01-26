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
#include "mptable.h"
#include "hash.h"

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
	new_block->epoch = ACCESS_ONCE(bonsai->desc->epoch);
	new_block->next = 0;

	if (likely(old_val)) {
		old_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, old_val);
		old_block->next = LOG_REGION_ADDR_TO_OFF(region, new_block);
		bonsai_flush((void*)old_block->next, sizeof(__le64), 0);
	}

	asm volatile("lock; incl %0" : "+m" (page->p_num_blk));
	bonsai_flush((void*)page->p_num_blk, sizeof(__le64), 1);

	return new_block;
}

struct oplog* alloc_oplog(struct log_region* region, int cpu) {
	struct oplog_blk* block = region->curr_blk;
	struct log_layer* layer = LOG(bonsai);
	struct oplog* log;

	if (unlikely(!block || block->cnt == NUM_OPLOG_PER_BLK)) {		
		block = alloc_oplog_block(cpu);
		region->curr_blk = block;
	}

	log = &block->logs[block->cnt++];

	if (!atomic_read(&layer->checkpoint) 
			&& atomic_add_return(1, &layer->nlogs) >= CHKPT_NLOG_INTERVAL) {
		wakeup_master();
	}

	return log;
}

struct oplog* oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, 
			int cpu, struct mptable* mptable, struct pnode* pnode) {
	struct log_layer* layer = LOG(bonsai);
	unsigned int epoch;
	struct log_region *region;
	struct oplog_blk* block;
	struct oplog* log;

	region = &layer->region[cpu];
retry:
	log = alloc_oplog(region, cpu);
	
	block = OPLOG_BLK(log);
	epoch = block->epoch;

	log->o_type = cpu_to_le8(op);
	log->o_numa_node = cpu_to_le8(numa_node);
	log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_addr = (__le64)pnode;
	log->o_kv.k = key;
	log->o_kv.v = val;

	if (unlikely(epoch != ACCESS_ONCE(bonsai->desc->epoch))) {
		/* an epoch passed */
		goto retry;
	}

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
		asm volatile("lock; decl %0" : "+m" (page->p_num_blk));
			
		if (!ACCESS_ONCE(page->p_num_blk)) {
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
 
	for (i = 0; i < NUM_PFLUSH_HASH_BUCKET; i ++) {
		bucket = &layer->buckets[i];
		spin_lock(&bucket->lock);
		hlist_for_each_entry_safe(e, hnode, tmp, &bucket->head, node) {
			hlist_del(&e->node);
			free(e);
		}
		spin_unlock(&bucket->lock);
	}
}

static int worker_oplog_merge(void *arg) {
	struct merge_work* mwork = (struct merge_work*)arg;
	struct log_layer* layer;
	struct oplog_blk* block;
	struct oplog *log;
	struct hbucket* bucket;
	struct hlist_node* hnode;
	struct log_region* region;
	merge_ent* e;
	pkey_t key;
	unsigned int i, j;
	int nlog = 0, nblk = 0;
	int count = 0, ret = 0;

	if (unlikely(!mwork))
		return;

	layer = mwork->layer;

	bonsai_print("pflush thread[%d] merge %d log lists\n", __this->t_id, mwork->count);
	
	for (i = 0; i < mwork->count; i ++) {
		block = mwork->first_blks[i];
		do {
			count = block->cnt;
			for (j = 0; j < count; j ++) {
				log = &block->logs[j];
				key = log->o_kv.k;
				bucket = &layer->buckets[p_hash(key)];

				try_free_log_page(block, &mwork->page_list);

				nlog++; atomic_dec(&layer->nlogs);
				bonsai_debug("pflush thread[%d] merge <%lu, %lu> in bucket[%d]\n", 
					__this->t_id, log->o_kv.k, log->o_kv.v, p_hash(key));
				
				spin_lock(&bucket->lock);
				hlist_for_each_entry(e, hnode, &bucket->head, node) {
					if (!key_cmp(e->log->o_kv.k, key)) {
						if (ordo_cmp_clock(e->log->o_stamp, log->o_stamp) == ORDO_LESS_THAN)
							/* less than */
							e->log = log;			
						continue;
					}
				}

				e = malloc(sizeof(merge_ent));
				e->log = log;
				hlist_add_head(&e->node, &bucket->head);
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
	bonsai_print("pflush thread[%d] merge %d logs %d blocks\n", __this->t_id, nlog, nblk);
	return 0;
}

static int worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct log_layer* layer = fwork->layer;
	struct hbucket* bucket;
	struct hlist_node *hnode, *tmp;
	struct oplog* log;
	merge_ent* e;
	unsigned int i;
	int cpu, node, count = 0;
	struct pnode* pnode;
	struct mptable* m;
	int ret = 0;
	struct numa_table* tables;
	pval_t* addr;

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
				pnode = (struct pnode*)log->o_addr;
				tables = pnode->table;
				cpu = get_cpu();
				addr = __mptable_lookup(tables, log->o_kv.k, cpu);
				if (!addr && tables->forward) {
					addr = __mptable_lookup(tables->forward, log->o_kv.k, cpu);
				}

				//FIXME: need lock
				if (addr == &log->o_kv.v) {
					for (node = 0; node < NUM_SOCKET; node ++) {
						m = MPTABLE_NODE(pnode->table, node);
						hs_remove(&m->hs, get_tid(), log->o_kv.k);
					}
				}

				pnode_remove(pnode, log->o_kv.k);
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
	free(fwork);
	bonsai_print("pflush thread[%d] flush %d logs\n", __this->t_id, count);

	return ret;
}

static void free_pages(struct log_layer *layer, struct list_head* page_list) {
	flush_page_struct *p, *n;
	int i = 0;
	
	list_for_each_entry_safe(p, n, page_list, list) {
		free_log_page(&layer->region[p->cpu], p->page);
		bonsai_debug("free log page[%d]: addr: %016lx count: %d\n", i++, p->page, p->page->p_num_blk);
		free(p);
	}
}

/*
 * oplog_flush: perform a full operation log flush
 */
void oplog_flush() {
	struct log_layer *l_layer = LOG(bonsai);
	struct data_layer *d_layer = DATA(bonsai);
	struct log_region *region;
	struct oplog_blk* first_blks[NUM_CPU];
	struct oplog_blk* curr_blks[NUM_CPU];
	struct oplog_blk* curr_blk;
	struct merge_work* mwork;
	struct merge_work* mworks[NUM_PFLUSH_THREAD];
	struct flush_work* fwork;
	struct work_struct* work;
	struct pnode* pnode;
	int i, j, cpu, cnt = 0, total = 0;
	int num_bucket_per_thread, num_region_per_thread, num_region_rest;

	bonsai_print("thread[%d]: start oplog flush [%d]\n", __this->t_id, l_layer->nflush);

	atomic_set(&l_layer->checkpoint, 1);

	/* 1. fetch and merge all log lists */
	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		region = &l_layer->region[cpu];
		curr_blk = ACCESS_ONCE(region->curr_blk);
		if (curr_blk && region->first_blk != curr_blk) {
			first_blks[cpu] = region->first_blk;
			curr_blks[cpu] = curr_blk;
			region->first_blk = curr_blk;
			total ++;
		}
	}

	if (unlikely(!total))
		goto out;

	if (unlikely(atomic_read(&l_layer->exit)))
		goto out;

	/* 2. merge all logs */
	if (total < NUM_PFLUSH_WORKER) {
		/* case I: total < (NUM_PFLUSH_THREAD - 1) */
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
			mworks[i] = NULL;
	} else {
		/* case II: total >= NUM_PFLUSH_WORKER */
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
		work->exec = worker_oplog_merge;
		work->exec_arg = (void*)(mworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);
	}

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit)))
		goto out;

	/* 3. flush all logs */
	num_bucket_per_thread = NUM_PFLUSH_HASH_BUCKET / (NUM_PFLUSH_THREAD - 1);
	for (cnt = 1; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		fwork = malloc(sizeof(struct flush_work));
		fwork->min_index = (cnt - 1) * num_bucket_per_thread;
		fwork->max_index = cnt * num_bucket_per_thread - 1;
		fwork->curr_index = fwork->min_index;
		fwork->layer = l_layer;

		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->exec = worker_oplog_flush;
		work->exec_arg = (void*)fwork;
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);
	}
	
	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit)))
		goto out;

	/* 4. traverse the pnode list */
	write_lock(&d_layer->lock);
	list_for_each_entry(pnode, &d_layer->pnode_list, list)
		pnode->stale = PNODE_DATA_CLEAN;
	write_unlock(&d_layer->lock);

	/* 5. free all pages */
	for (i = 1; i < NUM_PFLUSH_THREAD; i ++) {
		free_pages(l_layer, &mworks[i]->page_list);
		free(mworks[i]);
	}

	/* 6. finish */
	l_layer->nflush ++;
	atomic_set(&l_layer->checkpoint, 0);

out:
	bonsai_print("thread[%d]: finish log checkpoint [%d]\n", __this->t_id, l_layer->nflush);
	return;
}

static struct oplog* scan_one_cpu_log_region(struct log_region *region, pkey_t key) {
	struct oplog_blk* block;
	struct oplog *log;
	int i;

	if (region->curr_blk) {
		block = region->first_blk;
		while (1) {
			for (i = 0; i < block->cnt; i ++) {
				log = &block->logs[i];
				if (!key_cmp(log->o_kv.k, key)) {
					return log;
				}
			}
			if (block == region->curr_blk)
				return NULL;
			
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);
		}
	}
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
			if (log = scan_one_cpu_log_region(region, key)) {
				goto out;
			}
		}
	}

out:
	bonsai_print("log layer search key[%lu]: log %016lx\n", log ? log->o_kv.k : key, log);
	return log;
}
