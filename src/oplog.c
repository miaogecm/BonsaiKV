/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <stdlib.h>

#include "bonsai.h"
#include "oplog.h"
#include "ordo.h"
#include "region.h"

static int hash(pkey_t key) {
	return 0;
}

static struct oplog_blk* alloc_oplog_block(int cpu) {
	struct log_region* region = &LOG(bonsai)->region[cpu];
	struct log_region_desc* desc = region->desc;
	struct log_page_desc* page;
	struct oplog_blk *block, *old_block;

again:
	__le64 old_val = desc->r_oplog_top, new_val = old_val + sizeof(struct oplog_blk);
	
	if(cmpxchg(&desc->r_oplog_top, old_val, new_val)) {		
		if (unlikely(old_val | PAGE_MASK)) {
			/* we reach a page end */
			page = alloc_log_page(region);
			desc->r_oplog_top = LOG_REGION_ADDR_TO_OFF(region, page);
			goto again;
		}
		page = LOG_PAGE_DESC(old_val);
		page->p_num_blk ++;
	} else
		goto again;

	block = (struct oplog_blk*)(region->start + old_val);
	old_block = (struct oplog_blk*)old_val;

	memset(block, 0, sizeof(struct oplog_blk));
	block->cpu = cpu;
	old_block->next = LOG_REGION_ADDR_TO_OFF(region, block);

	return block;
}

struct oplog* alloc_oplog(struct log_region* region, pkey_t key, pval_t val, void* addr, optype_t type, int cpu) {
	struct oplog_blk* block = region->curr_blk;
	struct oplog* log;

	if (unlikely(block->cnt == NUM_OPLOG_PER_BLK)) {
		block = alloc_oplog_block(cpu);
		region->block = block;
	}

	log = block->oplog[block->cnt++];

	return log;
}

struct oplog* oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, struct mptable* mptable, struct pnode* pnode) {
	unsigned int epoch = LOG(bonsai)->epoch;
	struct log_region *region;
	struct oplog* log;
	int cpu = get_cpu();
	
	region = &LOG(bonsai)->region[cpu];
	log = alloc_oplog(region, key, val, addr, op, cpu);
	
	log->o_type = cpu_to_le8(type);
	log->o_numa_node = cpu_to_le8(numa_node);
	log->o_flag = pnode ? cpu_to_le8(1) : cpu_to_le8(0);
	log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_addr = log->o_flag ? pnode : table;
	log->o_kv.k = key;
	log->o_kv.v = (pkey_t) val;

	if (unlikely(epoch != LOG(bonsai)->epoch)) {
		/* an epoch passed */
		region->curr_blk->cnt--;
		bonsai_clfush(&region->block->cnt, sizeof(__le64), 1);
		return log;
	}

	if (unlikely(region->block->cnt == NUM_OPLOG_PER_BLK)) {
		/* persist it */
		bonsai_clfush(region->curr_blk, sizeof(struct oplog_blk), 1);
	}

	return log;
}

static void worker_oplog_merge(void *arg) {
	struct merge_work* mwork = (struct merge_work*)arg;
	struct log_layer* layer = mwork->layer;
	struct log_blk* block;
	struct oplog *log;
	struct hbucket* bucket;
	struct hlist_node* node;
	merge_ent* entry;
	pkey_t key;
	int i, j;
	
	for (i = 0, block = mwork->first_blk[i]; i < mwork->count; i ++) {
		do {
			for (j = 0; j < block->cnt; j ++) {
				log = &block->oplog[j];
				key = log->o_kv.k;
				bucket = &layer->buckets[hash(key)];
				spin_lock(&bucket->lock);
				hlist_for_each_entry(entry, node, &bucket->head, node) {
#ifndef BONSAI_SUPPORT_UPDATE
					if (cmp(merge_ent->log->o_kv.k, key)) {
						if (ordo_cmp_clock(merge_ent->log->o_stamp, log->o_stamp))
							/* less than */
							merge_ent->log = log;										
						goto next;
					}
#else
					/* TODO */
#endif
				}
				entry = malloc(sizeof(merge_ent));
				entry->log = log;
				hlist_add_head(&entry->node, &bucket->head);
				spin_unlock(&bucket->lock);
			}
next:
			block = LOG_REGION_OFF_TO_ADDR(layer->region[block->cpu], block->next);
		} while (block != mwork->last_blk[i]);
	}

	free(mwork);
}

static void worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct log_layer* layer = fwork->layer;
	struct log_page_desc *page;
	int ret, i, id = fwork->id;
	struct hbucket* bucket;
	struct hlist_node *node, *tmp;
	struct oplog_blk* block;
	struct oplog* log;
	merge_ent* entry;

	for (i = fwork->min_index; i < fwork->max_index; i ++) {
		bucket = &layer->bucket[i];
		hlist_for_each_entry_safe(entry, node, tmp, &bucket->head, node) {
			log = entry->log;
			switch(log->o_type) {
			case OP_INSERT:
			if (log->o_flag)
				ret = pnode_insert((struct pnode*)log->o_addr, NULL, 
						log->o_numa_node, log->o_kv.k, log->o_kv.v);
			else
				ret = pnode_insert(NULL, log->o_addr, 
						(struct numa_table*)log->o_numa_node, log->o_kv.k, log->o_kv.v);			
			break;
			case OP_REMOVE:
			ret = pnode_remove((struct pnode*)log->o_addr, log->o_kv.k);
			break;
			default:
			printf("bad operation type\n");
			break;
			}
			free(entry);

			block = OPLOG_BLK(log);
			asm volatile("lock; decl %0" : "+m" (block->cnt));
			
			if (!ACCESS_ONCE(block->cnt)) {
				page = LOG_PAGE_DESC(block);
				asm volatile("lock; decl %0" : "+m" (page->p_num_blk));
				if (!ACCESS_ONCE(page->p_num_blk))
					free_log_page(log->region[block->cpu], page);
			}
		}
	}

	free(fwork);
}

void oplog_flush(struct bonsai* bonsai) {
	struct log_layer *layer = LOG(bonsai);
	struct log_region *region;
	struct log_blk* first_blks[NUM_CPU];
	struct log_blk* curr_blks[NUM_CPU];
	struct merge_work* mwork;
	struct flush_work* fwork;
	int i, j, cnt = 0, num_region_per_thread;
	int num_bucket_per_thread;
	
	/* 1. fetch and merge all log lists */
	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		region = &layer->region[cpu];
		spin_lock(&region->lock);
		curr_blks[cpu] = region->curr_blk;
		first_blks[cpu] = region->first_blk;
		region->first_blk = region->curr_blk;
		spin_unlock(&region->lock);
	}

	/* 2. merge all logs */
	num_region_per_thread = NUM_CPU / NUM_PTHREAD;
	for (i = 0; i < NUM_PFLUSH_THREAD - 1; i ++) {
		mwork = malloc(sizeof(struct mwork));
		mwork->id = i;
		mwork->count = num_region_per_thread;
		while (j < num_region_per_thread) {
			mwork->first_blks[j] = first_blks[cnt];
			mwork->last_blks[j] = curr_blks[cnt];
			j ++; cnt ++;
		}
		j = 0;
	}
	mwork = malloc(sizeof(struct mwork));
	mwork->id = i;
	mwork->count = 0;
	while (cnt < NUM_CPU) {
		mwork->count ++;
		mwork->layer = layer;
		mwork->first_blks[j] = first_blks[cnt];
		mwork->last_blks[j] = curr_blks[cnt];
		j ++; cnt ++;
	}

	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_merge;
		work->arg = (void*)(mworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}

	wakeup_workers();

	park_master();

	/* 3. flush all logs */
	num_bucket_per_thread = MAX_HASH_BUCKET / NUM_PFLUSH_THREAD;
	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		fwork = malloc(sizeof(struct flush_work));
		fwork->id = cnt;
		fwork->min_index = cnt * num_bucket_per_thread;
		fwork->max_index = (cnt + 1) * num_bucket_per_thread;
		fwork->layer = layer;

		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_flush;
		work->arg = (void*)fworks;
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}
	
	wakeup_workers();

	park_master();

	/* 4. finish */
	printf("finish log checkpoint\n");
}
