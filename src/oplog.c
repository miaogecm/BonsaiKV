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

static struct oplog* alloc_oplog(pkey_t key, pval_t val, optype_t type) {
	struct oplog* log = malloc(sizeof(struct oplog));

	log->o_stamp = ordo_new_clock();
	log->o_type = type;
	log->o_func = ;
	log->o_kv.key = key;
	log->o_kv.val = val;
	INIT_LIST_HEAD(&o_list);
}

int oplog_insert(pkey_t key, pval_t val, void *table) {
	int cpu = get_cpu();
	struct list_head *head;
	struct oplog* log;
	
	log = alloc_oplog(OP_INSERT);
	head = &LOG(bonsai).oplogs[cpu];

	list_add_tail(&log->list, head);

	mtable_update(table, key, &log->o_kv.val);
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
