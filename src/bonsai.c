/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h> 

#include "bonsai.h"
#include "numa_config.h"

static struct bonsai_info* bonsai;

static char* bonsai_fpath = "/mnt/ext4/bonsai";

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

	log_region_init(layer, bonsai->desc)

	INIT_LIST_HEAD(&layer->mtable_list);

	layer->insert = mtable_insert;
	layer->remove = mtable_remove;
	layer->lookup = mtable_lookup;
}

static void log_layer_deinit(struct log_layer* layer) {
	struct mtable *table, *tmp;
	int cpu, ret;

	ret = log_region_deinit(layer);

	for (cpu = 0; cpu < NUM_CPU; cpu ++) {
		assert(list_empty(&layer->log_list[cpu]));
	}

	list_for_each_entry_safe(table, tmp, &layer->mtable_list, list) {
		mapping_table_free(table);
	}
}

static int data_layer_init(struct data_layer* layer) {
	int ret;

	ret = data_region_init(layer);

	INIT_LIST_HEAD(&layer->pnode_list);

	layer->insert = pnode_insert;
	layer->remove = pnode_remove;
	layer->lookup = pnode_lookup;
	layer->scan = pnode_scan;
}

static void data_layer_deinit(struct data_layer* layer) {
	/*TODO*/
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

void bonsai_deinit() {

	index_layer_deinit(&bonsai->i_layer);
    log_layer_deinit(&bonsai->l_layer);
    data_layer_deinit(&bonsai->d_layer);

	munmap((void*)bonsai->desc, PAGE_SIZE);
	close(bonsai->fd);

	free(bonsai);
}

int bonsai_init() {
	int error = 0, fd;
	char *addr;

	if ((fd = open(bonsai_fpath, O_CREAT|O_RDWR, 0666)) < 0) {
		perror("open\n");
		error = -EOPEN;
		goto out;	
	}
	
	if ((error = posix_fallocate(fd, 0, PAGE_SIZE)) != 0) {
		perror("posix_fallocate");
		goto out;
	}

	addr = mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_FILE, fd, 0);
	if (addr = MAP_FAILED) {
		perror("mmap");
		error = -EMMAP;
	}

    bonsai = malloc(sizeof(struct bonsai_info));
	bonsai->fd = fd;
	bonsai->desc = (struct bonsai_desc*)addr;

    if (!bonsai->desc->init) {
        index_layer_init(&bonsai->i_layer);
        log_layer_init(&bonsai->l_layer);
        data_layer_init(&bonsai->d_layer);

		bonsai->desc->init = 1;
    } else {
        bonsai_recover(bonsai);
    }

	epoch_init();
	bonsai_thread_init(bonsai);

out:
	return error;
}
