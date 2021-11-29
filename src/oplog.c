/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */

#include "bonsai.h"
#include "oplog.h"
#include "ordo.h"
#include "region.h"

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

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
		page->p_num_log ++;
	} else
		goto again;

	block = (struct oplog_blk*)(region->start + old_val);
	old_block = (struct oplog_blk*)old_val;

	memset(block, 0, sizeof(struct oplog_blk));
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

int oplog_insert(pkey_t key, pval_t val, optype_t op) {
	unsigned int epoch = LOG(bonsai)->epoch;
	struct log_region *region;
	int cpu = get_cpu();
	
	region = &LOG(bonsai)->region[cpu];
	log = alloc_oplog(region, key, val, addr, op, cpu);
	
	log->o_type = type;
	log->o_stamp = ordo_new_clock(0);
	log->o_node = 0;
	log->o_kv.key = key;
	log->o_kv.val = (pkey_t) val;

	if (unlikely(epoch != LOG(bonsai)->epoch)) {
		/* an epoch passed */
		region->curr_blk->cnt--;
		clfush(&region->block->cnt, sizeof(__le64));
		mfence();
		return 0;
	}

	if (unlikely(region->block->cnt == NUM_OPLOG_PER_BLK)) {
		/* persist it */
		clfush(region->curr_blk, sizeof(struct oplog_blk));
		mfence();
	}

	return 0;
}

static void worker_oplog_merge(void *arg) {
	struct merge_work* mwork = (struct merge_work*)arg;
		
	free(mwork);
}

static void worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct oplog* log = fwork->from;
	int ret;

	list_for_each_entry_from(log, fwork->head, list) {
		switch(log->o_type) {
		case OP_INSERT:
		ret = pnode_insert(log->o_node, log->o_kv.key, log->o_kv.val);
		break;
		case OP_REMOVE:
		ret = pnode_remove(log->o_node, log->o_kv.key);
		break;
		default:
		printf("bad operation type\n");
		break;
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
	int i, j, cnt = 0, num_blk_per_thread;
	
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
	num_blk_per_thread = NUM_CPU / NUM_PTHREAD;
	for (i = 0; i < NUM_PFLUSH_THREAD - 1; i ++) {
		mwork = malloc(sizeof(struct mwork));
		mwork->id = i;
		while (j < num_blk_per_thread) {
			mwork->first_blks[j] = first_blks[cnt];
			mwork->last_blks[j] = curr_blks[cnt];
			j ++; cnt ++;
		}
		j = 0;
	}
	mwork = malloc(sizeof(struct mwork));
	mwork->id = i;
	while (cnt < NUM_CPU) {
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
	

	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_flush;
		work->arg = (void*)(fworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}
	
	wakeup_workers();

	park_master();
}
