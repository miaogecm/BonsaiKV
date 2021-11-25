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

static struct oplog* alloc_oplog(pkey_t key, pval_t val, void* addr, optype_t type) {
	struct oplog* log = malloc(sizeof(struct oplog));

	log->o_stamp = ordo_new_clock(0);
	log->o_type = type;
	log->o_addr = addr;
	log->o_kv.key = key;
	log->o_kv.val = (pkey_t) val;
	INIT_LIST_HEAD(&log->o_list);
}

struct oplog* oplog_insert(pkey_t key, pval_t val, void* addr, optype_t op) {
	int cpu = get_cpu();
	struct list_head *head;
	struct oplog* log;
	
	log = alloc_oplog(key, val, addr, op);
	head = &LOG(bonsai).oplogs[cpu];

	list_add_tail(&log->list, head);
}

int oplog_remove(pkey_t key, void *table) {
	int cpu = get_cpu();
	struct list_head *head;
	struct oplog* log;
	
	log = alloc_oplog(OP_REMOVE);
	head = &LOG(bonsai).oplogs[cpu];

	list_add_tail(&log->list, head);

	mtable_update(table, key, NULL);
}

pval_t oplog_lookup(pkey_t key, void *table) {
	return mtable_lookup(table, key);
}

static void worker_oplog_flush(void* arg) {
	struct list_head *head = (struct list_head*)arg;
	struct oplog* log;
	int ret;

	list_for_each_entry(log, head, list) {
		switch(log->o_type) {
		case OP_INSERT:
		ret = pnode_insert(log->o_node, log->o_kv.key, log->o_kv.val);
		break;
		case OP_REMOVE:
		ret = pnode_remove(log->o_node, log->o_);
		break;
		default:
		printf("bad op\n");
		break;
		}
	}
}

static void list_insert_behind(struct list_head* node, struct list_head* pos) {
	
}

void oplog_flush(struct bonsai* bonsai) {
	struct log_layer *layer = LOG(bonsai);
	struct list_head heads[NUM_CPU], head;
	struct oplog *log, *tmp, *pos;
	int cpu, cnt, n, i, sum = 0;

	INIT_LIST_HEAD(&head);
	
	/* 1. fetch all log lists */
	for (cpu = 0; cpu < bonsai->total_cpu; cpu ++) {
		spin_lock(&layer->locks[cpu]);
		heads[cpu] = layer->log_list[cpu];
		INIT_LIST_HEAD(&layer->log_list[cpu]);
		spin_unlock(&layer->locks[cpu]);
	}

	/* 2. sort all logs */
	for (cpu = 0; cpu < bonsai->total_cpu; cpu ++) {
		list_for_each_entry_safe(log, tmp, &heads[cpu], list) {
			list_for_each_entry(pos, &head, list) {
				if (ordo_cmp_clock(pos->o_stamp, log->o_stamp)
					break;
			}
			list_del(log);
			list_insert_behind(&log->list, &pos->list);
			
			sum ++;
		}
	}

	n = sum / NUM_PFLUSH_THREAD; i = 0;
	for (cnt = 0; cnt < NUM_PFLUSH_THREAD - 1; cnt ++) {
		list_for_each_entry(log, &head, list) {
			if (i ++ > n) {
				heads[cnt] = ;
			}
		}
	}

	/* 3. dispatch works to threads */
	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		struct work_struct* work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_flush;
		work->arg = (void*)(&heads[cnt]);
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}
	
	wakeup_workers();

	park_master();

	/* 4. commit all changes */
	list_for_each_entry_safe(log, tmp, &head, list) {
		commit_bitmap(log->o_node, int pos, log->o_type);
		list_del(&log->o_list);
		free(log);
	}
}
