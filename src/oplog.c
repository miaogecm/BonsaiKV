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

#include "bonsai.h"
#include "oplog.h"
#include "ordo.h"
#include "arch.h"
#include "cpu.h"
#include "region.h"
#include "common.h"
#include "thread.h"
#include "pnode.h"

extern struct bonsai_info *bonsai;

typedef struct {
	int cpu;
	struct log_page_desc* page;
	struct list_head list;
} flush_page_struct;

struct oplog_blk* alloc_oplog_block(int cpu, pkey_t key) {
	struct log_layer* layer = LOG(bonsai);
	struct log_region* region = &layer->region[cpu];
	struct log_region_desc* desc = region->desc;
	struct log_page_desc* page;
	struct oplog_blk *new_block, *old_block;
	__le64 old_val, new_val;

again:
	old_val = desc->r_oplog_top; new_val = old_val + sizeof(struct oplog_blk);

	if (unlikely(!(old_val & ~PAGE_MASK))) {
		/* we reach a page end or it is first time to allocate an oplog block */
		page = alloc_log_page(region);
		bonsai_debug("new page: %lu %016lx\n", key, page);
		new_val = LOG_REGION_ADDR_TO_OFF(region, page) + 2 * sizeof(struct oplog_blk);
	} else {
		old_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, old_val);
		page = LOG_PAGE_DESC(old_block);
	}

	if (cmpxchg(&desc->r_oplog_top, old_val, new_val) != old_val)
		goto again;

	new_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, new_val - sizeof(struct oplog_blk));
	new_block->flush = 0;
	new_block->cnt = 0;
	new_block->cpu = cpu;
	new_block->next = 0;

	if (likely(old_val)) {
		old_block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, old_val);
		old_block->next = LOG_REGION_ADDR_TO_OFF(region, new_block);
		bonsai_flush((void*)old_block->next, sizeof(__le64), 0);
	}

	page->p_num_blk ++;
	bonsai_flush((void*)page->p_num_blk, sizeof(__le64), 1);

	return new_block;
}

struct oplog* alloc_oplog(struct log_region* region, pkey_t key, pval_t val, optype_t type, int cpu) {
	struct oplog_blk* block = region->curr_blk;
	struct oplog* log;

	if (unlikely(!block || block->cnt == NUM_OPLOG_PER_BLK || block->flush)) {		
		block = alloc_oplog_block(cpu, key);
		region->curr_blk = block;
	}

	log = &block->logs[block->cnt++];

	return log;
}

struct oplog* oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, 
			int cpu, struct mptable* mptable, struct pnode* pnode) {
	struct log_layer* layer = LOG(bonsai);
	unsigned int epoch = layer->epoch;
	struct log_region *region;
	struct oplog* log;

	region = &layer->region[cpu];
	log = alloc_oplog(region, key, val, op, cpu);

	log->o_type = cpu_to_le8(op);
	log->o_numa_node = cpu_to_le8(numa_node);
	log->o_flag = pnode ? cpu_to_le8(1) : cpu_to_le8(0);
	log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_addr = log->o_flag ? (__le64)pnode : (__le64)mptable;
	log->o_kv.k = key;
	log->o_kv.v = val;

	if (unlikely(epoch != layer->epoch)) {
		/* an epoch passed */
		region->curr_blk->cnt--;
		bonsai_flush(&region->curr_blk->cnt, sizeof(__le64), 1);
		return log;
	}

	if (unlikely(region->curr_blk->cnt == NUM_OPLOG_PER_BLK)) {
		/* persist it */
		region->curr_blk->flush = cpu_to_le8(1);
		bonsai_flush(region->curr_blk, sizeof(struct oplog_blk), 1);
	}

	bonsai_debug("thread [%d] insert an oplog <%lu, %lu>\n", __this->t_id, key, val);

	return log;
}

static void worker_oplog_merge(void *arg) {
	struct merge_work* mwork = (struct merge_work*)arg;
	struct log_layer* layer = mwork->layer;
	struct oplog_blk* block;
	struct oplog *log;
	struct hbucket* bucket;
	struct hlist_node* hnode;
	struct log_region* region;
	merge_ent* e;
	pkey_t key;
	unsigned int i, j;

	bonsai_debug("thread[%d] merge %d log lists\n", __this->t_id, mwork->count);
	
	for (i = 0, block = mwork->first_blks[i]; i < mwork->count; i ++) {
		do {
			//bonsai_debug("thread[%d] merge log block: %016lx count: %lu cpu: %d\n", __this->t_id, block, block->cnt, block->cpu);
			for (j = 0; j < block->cnt; j ++) {
				log = &block->logs[j];
				key = log->o_kv.k;
				//bonsai_debug("thread[%d] merge log <%lu %lu>\n", __this->t_id, log->o_kv.k, log->o_kv.v);
				bucket = &layer->buckets[simple_hash(key)];
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
			
			region = &layer->region[block->cpu];
			block = (struct oplog_blk*)LOG_REGION_OFF_TO_ADDR(region, block->next);
		} while (block != mwork->last_blks[i]);
	}
	
	free(mwork);
}

static void worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct log_layer* layer = fwork->layer;
	struct log_page_desc *page;
	struct hbucket* bucket;
	struct hlist_node *node, *tmp;
	struct oplog_blk* block;
	struct oplog* log;
	merge_ent* e;
	unsigned int i;

	bonsai_debug("thread[%d] flush bucket [%u %u]\n", __this->t_id, fwork->min_index, fwork->max_index);

	for (i = fwork->min_index; i <= fwork->max_index; i ++) {
		bucket = &layer->buckets[i];
		hlist_for_each_entry_safe(e, node, tmp, &bucket->head, node) {
			log = e->log;
			switch(log->o_type) {
			case OP_INSERT:
			if (log->o_flag)
				pnode_insert((struct pnode*)log->o_addr, NULL, 
						log->o_numa_node, log->o_kv.k, log->o_kv.v);
			else
				pnode_insert(NULL, (struct numa_table*)log->o_addr, 
						log->o_numa_node, log->o_kv.k, log->o_kv.v);			
			break;
			case OP_REMOVE:
			pnode_remove((struct pnode*)log->o_addr, log->o_kv.k);
			break;
			default:
			perror("bad operation type\n");
			break;
			}
			free(e);

			block = OPLOG_BLK(log);
			asm volatile("lock; decl %0" : "+m" (block->cnt));
			
			if (!ACCESS_ONCE(block->cnt)) {
				page = LOG_PAGE_DESC(block);
				asm volatile("lock; decl %0" : "+m" (page->p_num_blk));
				if (!ACCESS_ONCE(page->p_num_blk)) {
					flush_page_struct *p = malloc(sizeof(flush_page_struct));
					p->cpu = block->cpu;
					p->page = page;
					list_add(&p->list, fwork->flush_list);
				}
			}
		}
	}

	free(fwork);
}

static void free_pages(struct log_layer *layer, struct list_head* flush_list) {
	flush_page_struct *p, *n;
	
	list_for_each_entry_safe(p, n, flush_list, list) {
		free_log_page(&layer->region[p->cpu], p->page);
		free(p);
	}

	free(flush_list);
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
	struct merge_work* mwork;
	struct merge_work* mworks[NUM_PFLUSH_THREAD];
	struct flush_work* fwork;
	struct work_struct* work;
	struct list_head* flush_list;
	struct pnode* pnode;
	int i, j, cpu, cnt = 0, num_region_per_thread;
	int num_bucket_per_thread;

	bonsai_debug("oplog_flush\n");

	/* 1. fetch and merge all log lists */
	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		region = &l_layer->region[cpu];
		spin_lock(&region->lock);
		first_blks[cpu] = region->first_blk;
		curr_blks[cpu] = region->curr_blk;
		region->first_blk = region->curr_blk;
		spin_unlock(&region->lock);
	}

	/* 2. merge all logs */
	num_region_per_thread = NUM_CPU / (NUM_PFLUSH_THREAD - 1);
	for (i = 1, j = 0; i < NUM_PFLUSH_THREAD; i ++) {
		mwork = malloc(sizeof(struct merge_work));
		mwork->count = num_region_per_thread;
		mwork->layer = l_layer;
		while (j < num_region_per_thread) {
			mwork->first_blks[j] = first_blks[cnt];
			mwork->last_blks[j] = curr_blks[cnt];
			j ++; cnt ++;
		}
		j = 0;
		mworks[i] = mwork;
	}

	for (cnt = 1; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_merge;
		work->arg = (void*)(mworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);
	}

	wakeup_workers();

	park_master();

	/* 3. flush all logs */
	num_bucket_per_thread = NUM_CPU / (NUM_PFLUSH_THREAD - 1);
	flush_list = malloc(sizeof(struct list_head));
	INIT_LIST_HEAD(flush_list);
	for (cnt = 1; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		fwork = malloc(sizeof(struct flush_work));
		fwork->min_index = (cnt - 1) * num_bucket_per_thread;
		fwork->max_index = cnt * num_bucket_per_thread - 1;
		fwork->flush_list = flush_list;
		fwork->layer = l_layer;

		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_flush;
		work->arg = (void*)fwork;
		workqueue_add(&bonsai->pflushd[cnt]->t_wq, work);
	}
	
	wakeup_workers();

	park_master();

	/* 4. traverse the pnode list */
	list_for_each_entry(pnode, &d_layer->pnode_list, list)
		pnode->stale = PNODE_DATA_CLEAN;

	/* 5. free all pages */
	free_pages(l_layer, flush_list);

	/* 6. finish */
	l_layer->nflush ++;
	
	bonsai_debug("finish log checkpoint %d\n", l_layer->nflush);
}
