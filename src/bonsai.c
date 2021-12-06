/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h> 

#include "bonsai.h"
#include "numa_config.h"
#include "mptable.h"
#include "pnode.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/bonsai";

static void index_layer_init(struct index_layer* layer, init_func_t init, insert_func_t insert, remove_func_t remove, lookup_func_t lookup, scan_func_t scan, destory_func_t destroy) {
	//layer->index_struct = init();

	layer->insert = insert;
	layer->remove = remove;
	layer->lookup = lookup;
	layer->scan = scan;
	layer->destory = destroy;
}

static void index_layer_deinit(struct index_layer* layer) {
	layer->destory(layer->index_struct);
}

static int log_layer_init(struct log_layer* layer) {
	int i, err = 0;

	err = epoch_init();
	if (err)
		goto out;

	err = log_region_init(layer, bonsai->desc);
	if (err)
		goto out;

	INIT_LIST_HEAD(&layer->mptable_list);

	for (i = 0; i < MAX_HASH_BUCKET; i ++) {
		INIT_HLIST_HEAD(&layer->buckets[i].head);
		spin_lock_init(&layer->buckets[i].lock);
	}

	layer->insert = mptable_insert;
	layer->update = mptable_update;
	layer->remove = mptable_remove;
	layer->lookup = mptable_lookup;

out:
	return err;
}

static void log_layer_deinit(struct log_layer* layer) {
	struct mptable *table, *tmp;
	int cpu, ret;

	log_region_deinit(layer);

	list_for_each_entry_safe(table, tmp, &layer->mptable_list, list) {
		list_del(&table->list);
		numa_mptable_free(table);
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

int bonsai_insert(pkey_t key, pval_t value) {
	struct numa_table *tables, **addr;

	addr = INDEX(bonsai)->lookup(key);
	if (unlikely(!*addr)) {
		*addr = numa_mptable_alloc(tables, &LOG(bonsai)->mptable_list);
	}
	tables = *addr;

	return LOG(bonsai)->insert(MPTABLE_NODE(tables, get_numa_node()), key, value);
}

int bonsai_update(pkey_t key, pval_t value) {
	struct numa_table *tables, **addr;

	addr = INDEX(bonsai)->lookup(key);
	if (unlikely(!*addr)) {
		*addr = numa_mptable_alloc(tables, &LOG(bonsai)->mptable_list);
	}
	tables = *addr;

	return LOG(bonsai)->update(MPTABLE_NODE(tables, get_numa_node()), key, value);
}

int bonsai_remove(pkey_t key) {
	struct numa_table *tables, **addr;

	addr = INDEX(bonsai)->lookup(key);
	if (unlikely(!*addr)) {
		return -ENOENT;
	}
	tables = *addr;

	return LOG(bonsai)->remove(MPTABLE_NODE(tables, get_numa_node()), key);
}

pval_t bonsai_lookup(pkey_t key) {
	struct numa_table *tables, **addr;;
	pval_t value;
	
	addr = INDEX(bonsai)->lookup(key);
	if (unlikely(!*addr)) {
		return -ENOENT;
	}
	tables = *addr;

	value = LOG(bonsai)->lookup(tables, key, table);

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
        index_layer_init(&bonsai->i_layer, NULL, NULL, NULL, NULL, NULL, NULL);
        log_layer_init(&bonsai->l_layer);
        data_layer_init(&bonsai->d_layer);

		bonsai->desc_init = 1;
    } else {
        bonsai_recover(bonsai);
    }

	epoch_init();
	bonsai_thread_init(bonsai);

out:
	return error;
}
