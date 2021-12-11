/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h> 

#include "bonsai.h"
#include "numa_config.h"
#include "hash_set.h"
#include "mptable.h"
#include "pnode.h"
#include "epoch.h"
#include "cpu.h"

#include "../index/kv.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/bonsai";


static void sentinel_node_init() {
	struct numa_table* table;
	int node = get_numa_node(get_cpu());
	struct index_layer *i_layer = INDEX(bonsai);
	
	table = numa_mptable_alloc(&bonsai->l_layer);
	i_layer->insert(i_layer->index_struct, ULONG_MAX, (void*)table);
	
	hs_insert(&MPTABLE_NODE(table, node)->hs, get_tid(), ULONG_MAX, NULL);
	
	pnode_insert(NULL, table, node, ULONG_MAX, ULONG_MAX);
}

static void index_layer_init(char* index_name, struct index_layer* layer, init_func_t init, 
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan, destory_func_t destroy) {
	
	layer->index_struct = init();

	layer->insert = insert;
	layer->remove = remove;
	layer->lookup = lookup;
	layer->scan = scan;
	layer->destory = destroy;

	printf("index_layer_init: %s\n", index_name);
}

static void index_layer_deinit(struct index_layer* layer) {
	layer->destory(layer->index_struct);

	printf("index_layer_deinit\n");
}

static int log_layer_init(struct log_layer* layer) {
	int i, err = 0;

	layer->epoch = 0;
	err = epoch_init();
	if (err)
		goto out;

	layer->nflush = 0;
	err = log_region_init(layer, bonsai->desc);
	if (err)
		goto out;

	spin_lock_init(&layer->lock);
	INIT_LIST_HEAD(&layer->numa_table_list);

	for (i = 0; i < MAX_HASH_BUCKET; i ++) {
		INIT_HLIST_HEAD(&layer->buckets[i].head);
		spin_lock_init(&layer->buckets[i].lock);
	}

	printf("log_layer_init\n");

out:
	return err;
}

static void log_layer_deinit(struct log_layer* layer) {
	struct numa_table *table, *tmp;
	int node;

	log_region_deinit(layer);

	list_for_each_entry_safe(table, tmp, &layer->numa_table_list, list) {
		list_del(&table->list);

		for (node = 0; node < NUM_SOCKET; node ++) {
			hs_destroy(&MPTABLE_NODE(table, node)->hs);
		}
		
		numa_mptable_free(table);
	}

	printf("log_layer_deinit\n");
}

static int data_layer_init(struct data_layer* layer) {
	int ret;

	ret = data_region_init(layer);
	if (ret)
		goto out;

	spin_lock_init(&layer->lock);
	INIT_LIST_HEAD(&layer->pnode_list);

	printf("data_layer_init\n");

out:
	return ret;
}

static void data_layer_deinit(struct data_layer* layer) {
	struct pnode* pnode, *tmp;
	int i;

	list_for_each_entry_safe(pnode, tmp, &layer->pnode_list, list) {
		free(pnode->slot_lock);
		for (i = 0; i < NUM_BUCKET; i ++)
			free(pnode->bucket_lock[i]);

		list_del(&pnode->list);
	}
	
	data_region_deinit(layer);

	printf("data_layer_deinit\n");
}

int bonsai_insert(pkey_t key, pval_t value) {
	struct numa_table *table;
	int cpu = get_cpu(), numa_node = get_numa_node(cpu);
	struct index_layer* i_layer = INDEX(bonsai);

	table = (struct numa_table*)i_layer->lookup(i_layer->index_struct, key);
	
	assert(table);
	
	return mptable_insert(table, numa_node, cpu, key, value);
}

int bonsai_update(pkey_t key, pval_t value) {
	struct numa_table *table;
	int cpu = get_cpu(), numa_node = get_numa_node(cpu);
	struct index_layer* i_layer = INDEX(bonsai);

	table = (struct numa_table*)i_layer->lookup(i_layer->index_struct, key);

	assert(table);

	return mptable_update(table, numa_node, cpu, key, &value);
}

int bonsai_remove(pkey_t key) {
	struct numa_table *table;
	int cpu = get_cpu(), numa_node = get_numa_node(cpu);
	struct index_layer* i_layer = INDEX(bonsai);

	table = (struct numa_table*)i_layer->lookup(i_layer->index_struct, key);

	assert(table);
	
	return mptable_remove(table, numa_node, cpu, key);
}

pval_t bonsai_lookup(pkey_t key) {
	struct numa_table *table;
	struct index_layer* i_layer = INDEX(bonsai);
	int cpu = get_cpu();
	
	table = (struct numa_table*)i_layer->lookup(i_layer->index_struct, key);

	assert(table);

	return mptable_lookup(table, key, cpu);
}


int bonsai_scan(pkey_t low, pkey_t high) {
	struct index_layer* layer = INDEX(bonsai);

	return layer->scan(layer->index_struct, low, high);
}

void bonsai_deinit() {
	printf("bonsai deinit\n");
	
	bonsai_self_thread_exit();

	bonsai_pflushd_thread_exit();
	
	index_layer_deinit(&bonsai->i_layer);
    log_layer_deinit(&bonsai->l_layer);
    data_layer_deinit(&bonsai->d_layer);

	munmap((void*)bonsai->desc, PAGE_SIZE);
	close(bonsai->fd);

	free(bonsai);
}

int bonsai_init(char* index_name, init_func_t init, destory_func_t destroy,
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan) {
	int error = 0, fd;
	char *addr;

	if ((fd = open(bonsai_fpath, O_CREAT|O_RDWR, 0666)) < 0) {
		perror("open");
		error = -EOPEN;
		goto out;	
	}
	
	if ((error = posix_fallocate(fd, 0, PAGE_SIZE)) != 0) {
		perror("posix_fallocate");
		goto out;
	}

	addr = mmap(NULL, PAGE_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_FILE, fd, 0);
	if (addr == MAP_FAILED) {
		perror("mmap");
		error = -EMMAP;
		goto out;
	}

    bonsai = malloc(sizeof(struct bonsai_info));
	bonsai->fd = fd;
	bonsai->desc = (struct bonsai_desc*)addr;

    if (!bonsai->desc->init) {
		/* 1. initialize index layer */
        index_layer_init(index_name, &bonsai->i_layer, kv_init, 
			kv_insert, kv_remove, kv_lookup, kv_scan, kv_destory);

		/* 2. initialize log layer */
        error = log_layer_init(&bonsai->l_layer);
		if (error)
			goto out;

		/* 3. initialize data layer */
        error = data_layer_init(&bonsai->d_layer);
		if (error)
			goto out;

		/* 4. initialize self */
		bonsai_self_thread_init();
		
		/* 4. initialize sentinel node */
		sentinel_node_init();

		bonsai->desc->init = 1;
    } else {
        bonsai_recover();
    }

	/* 4. initialize pflush thread */
	bonsai_pflushd_thread_init();

	printf("bonsai is initialized successfully!\n");

out:
	return error;
}
