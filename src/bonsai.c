/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */

#include "bonsai.h"

static struct bonsai* bonsai;

static void index_layer_init(struct index_layer* layer, init_func_t init, insert_func_t insert, remove_func_t remove, lookup_func_t lookup, scan_func_t scan) {
	layer->index_struct = init();
	layer->insert = insert;
	layer->remove = remove;
	layer->lookup = lookup;
	layer->scan = scan;
}

static void log_layer_init(struct log_layer* layer) {
	int cpu;

	for (cpu = 0; cpu < NUM_CPU; cpu++) {
		INIT_LIST_HEAD(&layer->oplogs[cpu]);
	}

	INIT_LIST_HEAD(&layer->mtables);

	layer->insert = oplog_insert;
	layer->remove = oplog_remove;
	layer->lookup = oplog_lookup;
}

static void data_layer_init(struct data_layer* layer) {
	INIT_LIST_HEAD(&layer->pnodes);

	layer->insert = pnode_insert;
	layer->remove = pnode_remove;
	layer->lookup = pnode_lookup;
	layer->scan = pnode_scan;
}

static struct bonsai* get_bonsai() {
    PMEMoid root = pmemobj_root(pop[0], sizeof(struct bonsai));
    return pmemobj_direct(root);
}

int bonsai_insert(pkey_t key, pval_t val) {
	int ret = 0;

	ret = INDEX(bonsai)->insert(key, val);
	if (ret < 0)
		goto out;

	ret = LOG(bonsai)->insert(key, val);

out:
	return ret;
}

int bonsai_remove(pkey_t key) {
	int ret = 0;

	ret = INDEX(bonsai)->remove(key);
	if (ret < 0)
		goto out;

	ret = LOG(bonsai)->remove(key);

out:
	return ret;	
}

pval_t bonsai_lookup(pkey_t key) {
	pval_t value;
	
	value = INDEX(bonsai)->lookup(key);
	value = LOG(bonsai)->lookup(key);

	return value;
}

int bonsai_scan(pkey_t low, pkey_t high) {
	
	DATA(bonsai)->scan(low, high);
}

void bonsai_deinit(struct bonsai* bonsai) {
	
}

void bonsai_init(struct bonsai* bonsai) {
    bonsai = get_bonsai();

    if (!bonsai->bonsai_init) {
        index_layer_init(&bonsai->i_layer);
        log_layer_init(&bonsai->l_layer);
        data_layer_init(&bonsai->d_layer);
    } else {
        bonsai_recover(bonsai);
    }
}
