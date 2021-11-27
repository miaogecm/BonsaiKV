/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */

#include "bonsai.h"
#include "numa.h"

static struct bonsai* bonsai;

static void index_layer_init(struct index_layer* layer, init_func_t init, insert_func_t insert, remove_func_t remove, lookup_func_t lookup, scan_func_t scan, destory_func_t destory) {
	layer->index_struct = init();

	layer->insert = insert;
	layer->remove = remove;
	layer->lookup = lookup;
	layer->scan = scan;
	layer->destory = destroy;
}

static void index_layer_deinit(struct index_layer* layer) {
	layer->destory(layer->index_struct);
}

static void log_layer_init(struct log_layer* layer) {
	int cpu;

	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		atomic_set(&layer->num_log, 0);
		INIT_LIST_HEAD(&layer->oplogs[cpu]);
	}

	INIT_LIST_HEAD(&layer->mtable_list);

	layer->insert = mtable_insert;
	layer->remove = mtable_remove;
	layer->lookup = mtable_lookup;
}

static void log_layer_deinit(struct log_layer* layer) {
	struct mtable *table, *tmp;
	int cpu;

	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		assert(list_empty(&layer->log_list[cpu]));
	}

	list_for_each_entry_safe(table, tmp, &layer->mtable_list, list) {
		mapping_table_free(table);
	}
}

static void data_layer_init(struct data_layer* layer) {
	INIT_LIST_HEAD(&layer->pnode_list);

	layer->insert = pnode_insert;
	layer->remove = pnode_remove;
	layer->lookup = pnode_lookup;
	layer->scan = pnode_scan;
}

static void data_layer_deinit(struct data_layer* layer) {
	/*TODO*/
}

static struct bonsai* get_bonsai() {
    PMEMoid root = pmemobj_root(pop[0], sizeof(struct bonsai));
    return pmemobj_direct(root);
}

int bonsai_insert(pkey_t key, pval_t val) {
	int ret = 0;
	void* table;

	table = INDEX(bonsai)->lookup(key);
	if (ret < 0)
		goto out;

	ret = LOG(bonsai)->insert(key, val, table->pnode);

out:
	return ret;
}

int bonsai_remove(pkey_t key) {
	int ret = 0;
	void *table;

	ret = INDEX(bonsai)->lookup(key);
	if (ret < 0)
		goto out;

	ret = LOG(bonsai)->remove(key, table);

out:
	return ret;	
}

pval_t bonsai_lookup(pkey_t key) {
	pval_t value;
	void* table;
	
	table = INDEX(bonsai)->lookup(key);

	value = LOG(bonsai)->lookup(key, table);

	return value;
}

int bonsai_scan(pkey_t low, pkey_t high) {
	
	DATA(bonsai)->scan(low, high);
}

void bonsai_deinit(struct bonsai* bonsai) {

	index_layer_deinit(&bonsai->i_layer);
    log_layer_deinit(&bonsai->l_layer);
    data_layer_deinit(&bonsai->d_layer);

	free(bonsai);
}

void bonsai_init(struct bonsai* bonsai) {
    bonsai = get_bonsai();

    if (!bonsai->bonsai_init) {
		bonsai->total_cpu = sysconf(_SC_NPROCESSORS_ONLN);
		bonsai_thread_init(bonsai);

        index_layer_init(&bonsai->i_layer);
        log_layer_init(&bonsai->l_layer);
        data_layer_init(&bonsai->d_layer);
    } else {
        bonsai_recover(bonsai);
    }
}
