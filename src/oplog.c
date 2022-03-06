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

typedef struct {
	int cpu;
	struct log_page_desc* page;
	struct list_head list;
} flush_page_struct;

typedef struct log_ent {
    pkey_t key;
    unsigned long ts;
    struct oplog *log;
} log_ent;

struct oplog_work {
    int node, id;
	int count;
	struct list_head free_page_list;
	struct log_layer* layer;
	volatile struct oplog_blk* first_blks[NUM_CPU / NUM_PFLUSH_WORKER_PER_NODE];
	volatile struct oplog_blk* last_blks[NUM_CPU / NUM_PFLUSH_WORKER_PER_NODE];
};

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

struct oplog *oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, int cpu) {
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

#define num_cmp(x, y)           ((x) == (y) ? 0 : ((x) < (y) ? -1 : 1))
#define sort_cmp(a, b, arg)     (pkey_compare(((log_ent *) (a))->key, ((log_ent *) (b))->key) \
                                 ? : num_cmp(((log_ent *) (a))->ts, ((log_ent *) (b))->ts))
#include "sort.h"

static void worker_sort_log(struct log_layer *layer, int node, int id) {
	int phase, n = NUM_PFLUSH_WORKER_PER_NODE, i, left, oid, cnt, ocnt, delta;
    log_ent *ent, *oent, *nent, *nptr, *data, *odata, **get;

    data = layer->log_ents[node][id].ents;
    cnt = layer->log_ents[node][id].n;

    do_qsort(data, cnt, sizeof(log_ent), NULL);

	for (phase = 0; phase < n; phase++) {
        /* Wait for the last phase to be done. */
	    pthread_barrier_wait(&layer->barrier[node]);

		bonsai_print("thread <%d, %d> -----------------phase [%d]---------------\n", node, id, phase);

        /* Am I the left half in this phase? */
        left = id % 2 == phase % 2;
        delta = left ? 1 : -1;
        oid = id + delta;
        if (oid < 0 || oid >= NUM_PFLUSH_WORKER_PER_NODE) {
            pthread_barrier_wait(&layer->barrier[node]);
            continue;
        }

        ocnt = layer->log_ents[node][oid].n;
        odata = layer->log_ents[node][oid].ents;

        ent = left ? data : (data + cnt - 1);
        oent = left ? odata : (odata + ocnt - 1);

        nent = malloc(sizeof(log_ent) * cnt);
        nptr = left ? nent : (nent + cnt - 1);

        for (i = 0; i < cnt; i++) {
            if (unlikely(ent - data == cnt || ent < data)) {
                get = &oent;
            } else if (unlikely(oent - odata == ocnt || oent < odata)) {
                get = &ent;
            } else {
                if (!pkey_compare(ent->key, oent->key)) {
                    get = &ent;
                } else {
                    get = (!left ^ (pkey_compare(ent->key, oent->key) < 0)) ? &ent : &oent;
                }
            }
            *nptr = **get;
            *get += delta;
            nptr += delta;
        }

	    pthread_barrier_wait(&layer->barrier[node]);

        free(data);
        layer->log_ents[node][id].ents = data = nent;
	}
}

static int worker_oplog_collect_and_sort(void *arg) {
	struct oplog_work* owork = (struct oplog_work*)arg;
	struct log_layer* layer;
	volatile struct oplog_blk* block;
	struct oplog *log;
    struct log_region* region;
	int i, j, count, ret = 0;
	int nlog = 0, nblk = 0;
    log_ent *e;

	if (unlikely(!owork))
		goto out;

	layer = owork->layer;

	bonsai_print("pflush thread <%d, %d> collect and sort %d log lists\n", owork->node, owork->id, owork->count);

    /* 1. count logs */
	for (i = 0; i < owork->count; i ++) {
		block = owork->first_blks[i];
        assert(block != owork->last_blks[i]);
        do {
			count = block->cnt;

            nblk++;
            nlog += count;

			region = &layer->region[block->cpu];
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);

			if (unlikely(atomic_read(&layer->exit))) {
				ret = -EEXIT;
                nlog = 0;
				goto out;
			}
		} while(block != owork->last_blks[i]);
    }

    /* 2. collect logs */
    layer->log_ents[owork->node][owork->id].ents = e = malloc(sizeof(log_ent) * nlog);

	for (i = 0; i < owork->count; i ++) {
		block = owork->first_blks[i];

        assert(block != owork->last_blks[i]);

		do {
			count = block->cnt;
			
			for (j = 0; j < count; j ++) {
				log = &block->logs[j];

				try_free_log_page((struct oplog_blk*)block, &owork->free_page_list);

                atomic_dec(&layer->nlogs[block->cpu].cnt);

                e->key = log->o_kv.k;
                e->ts = log->o_stamp;
                e->log = log;
                e++;
			}

			region = &layer->region[block->cpu];
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);

			if (unlikely(atomic_read(&layer->exit))) {
				ret = -EEXIT;
                nlog = 0;
				goto out;
			}
		} while(block != owork->last_blks[i]);
	}

out:
    layer->log_ents[owork->node][owork->id].n = nlog;
    
	/* 3. sort */
	worker_sort_log(layer, owork->node, owork->id);

	bonsai_print("pflush thread <%d, %d> collect and sort %d logs %d blocks\n", owork->node, owork->id, nlog, nblk);
	return ret;
}

static int worker_oplog_flush(void* arg) {
	struct oplog_work* owork = arg;
	struct log_layer* layer = owork->layer;
	struct oplog* log;
	int count = 0, ret = 0, n, delta;
    log_ent *start, *end, *e;

	bonsai_print("pflush thread <%d, %d> start flush\n", owork->node, owork->id);

    n = layer->log_ents[owork->node][owork->id].n;
    start = layer->log_ents[owork->node][owork->id].ents;
    end = start + n;

    if (owork->node % 2) {
        e = end - 1;
        delta = -1;
    } else {
        e = start;
        delta = 1;
    }

	for (; n; n--, e += delta) {
		log = e->log;
		
		bonsai_debug("pflush thread[%d] flush <%lu %lu> [%s]\n", 
			__this->t_id, log->o_kv.k, log->o_kv.v, log->o_type ? "remove" : "insert");

		switch(log->o_type) {
		case OP_INSERT:
            pnode_insert(log->o_kv.k, &log->o_kv.v);
			break;
		case OP_REMOVE:
			pnode_remove(log->o_kv.k);
			break;
		default:
			perror("bad operation type\n");
			break;
		}

		count ++;

		if (unlikely(count % 10 == 0 && atomic_read(&layer->exit))) {
			ret = -EEXIT;
			goto out;
		}
	}

out:
    free(start);
	bonsai_print("pflush thread <%d, %d> flush %d logs\n", owork->node, owork->id, count);

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
    struct log_region *region;
	volatile struct oplog_blk* first_blks[NUM_CPU_PER_SOCKET];
	volatile struct oplog_blk* curr_blks[NUM_CPU_PER_SOCKET];
	volatile struct oplog_blk* curr_blk;
	struct oplog_work* owork;
	struct oplog_work* oworks[NUM_SOCKET][NUM_PFLUSH_WORKER_PER_NODE];
	struct work_struct* work;
    int i, j, cpu, cnt, total, node, cpu_idx;
    int num_region_per_thread, num_region_rest;
	int num_block_per_thread;
    int nlogs, empty = 1;
    struct oplog_work empty_owork[NUM_SOCKET][NUM_PFLUSH_WORKER_PER_NODE] = { };

	bonsai_print("thread[%d]: start oplog checkpoint [%d]\n", __this->t_id, l_layer->nflush);

	atomic_set(&l_layer->checkpoint, 1);

    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            empty_owork[node][i].layer = l_layer;
            empty_owork[node][i].node = node;
            empty_owork[node][i].id = i;
        }
    }

    /************************************************
     *   Per-node Fetch and Distribute Log Lists    *
     ************************************************/

    for (node = 0; node < NUM_SOCKET; node++) {
        total = 0;
        cnt = 0;

        /* 1. fetch and merge all log lists */
        nlogs = 0;
        for (cpu_idx = 0; cpu_idx < NUM_CPU_PER_SOCKET; cpu_idx++) {
            cpu = node_to_cpu(node, cpu_idx);
            region = &l_layer->region[cpu];
            nlogs += atomic_read(&l_layer->nlogs[cpu].cnt);
            curr_blk = ACCESS_ONCE(region->curr_blk);
            /* TODO: Non-full log blocks can not be flushed! */
            if (curr_blk && region->first_blk != curr_blk) {
                first_blks[total] = region->first_blk;
                curr_blks[total] = curr_blk;
                region->first_blk = curr_blk;
                total++;
            }
        }

        if (unlikely(!total)) {
            for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
                oworks[node][i] = &empty_owork[node][i];
            }
            continue;
        } else {
            empty = 0;
        }

        if (unlikely(atomic_read(&l_layer->exit))) {
            goto out;
        }

        /* 2. merge all logs */
        if (total == 1) {
            /* case I: total == 1 */
            num_region_per_thread = 1;
            num_block_per_thread = nlogs / NUM_OPLOG_PER_BLK / NUM_PFLUSH_WORKER_PER_NODE;
            curr_blk = first_blks[0];
            for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
                owork = malloc(sizeof(struct oplog_work));
                owork->count = num_block_per_thread ? num_region_per_thread : 0;
                owork->layer = l_layer;
                INIT_LIST_HEAD(&owork->free_page_list);
                owork->first_blks[0] = curr_blk;
                if (i != NUM_PFLUSH_WORKER_PER_NODE - 1) {
                    for (j = 0; j < num_block_per_thread; j++)
                        curr_blk = (struct oplog_blk *) LOG_REGION_OFF_TO_ADDR((&l_layer->region[curr_blk->cpu]),
                                                                               curr_blk->next);
                } else {
                    curr_blk = curr_blks[0];
                }
                owork->last_blks[0] = curr_blk;
                oworks[node][i] = owork;
            }
        } else if (total < NUM_PFLUSH_WORKER_PER_NODE) {
            /* case II: total < NUM_PFLUSH_WORKER_PER_NODE */
            num_region_per_thread = 1;
            for (i = 0, j = 0; i < total; i++) {
                owork = malloc(sizeof(struct oplog_work));
                owork->count = num_region_per_thread;
                owork->layer = l_layer;
                INIT_LIST_HEAD(&owork->free_page_list);
                while (j < num_region_per_thread) {
                    owork->first_blks[j] = first_blks[cnt];
                    owork->last_blks[j] = curr_blks[cnt];
                    j++;
                    cnt++;
                }
                j = 0;
                oworks[node][i] = owork;
            }
            for (i = total; i < NUM_PFLUSH_WORKER_PER_NODE; i++)
                oworks[node][i] = &empty_owork[node][i];
        } else {
            /* case III: total >= NUM_PFLUSH_WORKER_PER_NODE */
            num_region_per_thread = total / NUM_PFLUSH_WORKER_PER_NODE;
            for (i = 0, j = 0; i < NUM_PFLUSH_WORKER_PER_NODE - 1; i++) {
                owork = malloc(sizeof(struct oplog_work));
                owork->count = num_region_per_thread;
                owork->layer = l_layer;
                INIT_LIST_HEAD(&owork->free_page_list);
                while (j < num_region_per_thread) {
                    owork->first_blks[j] = first_blks[cnt];
                    owork->last_blks[j] = curr_blks[cnt];
                    j++;
                    cnt++;
                }
                j = 0;
                oworks[node][i] = owork;
            }
            j = 0;
            num_region_rest = total - (NUM_PFLUSH_WORKER_PER_NODE - 1) * num_region_per_thread;
            owork = malloc(sizeof(struct oplog_work));
            owork->count = num_region_rest;
            owork->layer = l_layer;
            INIT_LIST_HEAD(&owork->free_page_list);
            while (j < num_region_rest) {
                owork->first_blks[j] = first_blks[cnt];
                owork->last_blks[j] = curr_blks[cnt];
                j++;
                cnt++;
            }
            oworks[node][i] = owork;
        }
    }

    if (unlikely(empty)) {
        goto out;
    }

    /************************************************
     *      Per-thread collect and sort logs        *
     ************************************************/

    for (node = 0; node < NUM_SOCKET; node++) {
        for (cnt = 0; cnt < NUM_PFLUSH_WORKER_PER_NODE; cnt++) {
            oworks[node][cnt]->node = node;
            oworks[node][cnt]->id = cnt;
            work = malloc(sizeof(struct work_struct));
            INIT_LIST_HEAD(&work->list);
            work->exec = worker_oplog_collect_and_sort;
            work->exec_arg = (void *) (oworks[node][cnt]);
            workqueue_add(&bonsai->pflush_workers[node][cnt]->t_wq, work);
        }
    }

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 3. flush all logs */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (cnt = 0; cnt < NUM_PFLUSH_WORKER_PER_NODE; cnt++) {
            work = malloc(sizeof(struct work_struct));
            INIT_LIST_HEAD(&work->list);
            work->exec = worker_oplog_flush;
            work->exec_arg = (void *) (oworks[node][cnt]);
            workqueue_add(&bonsai->pflush_workers[node][cnt]->t_wq, work);
        }
    }

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 4. free all pages */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 1; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            if (oworks[node][i] < &empty_owork[0][0] ||
                oworks[node][i] > &empty_owork[NUM_SOCKET - 1][NUM_PFLUSH_WORKER_PER_NODE - 1]) {
                free_pages(l_layer, &oworks[node][i]->free_page_list);
                free(oworks[node][i]);
            }
        }
    }

out:
	/* 5. finish */
	l_layer->nflush ++;
	atomic_set(&l_layer->checkpoint, 0);

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
