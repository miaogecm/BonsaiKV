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
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h> 

#include "bonsai.h"
#include "numa_config.h"
#include "per_node.h"
#include "hash_set.h"
#include "mptable.h"
#include "pnode.h"
#include "epoch.h"
#include "cpu.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/bonsai";

extern void* index_struct(void* index_struct);
extern void kv_print(void* index_struct);

static void index_layer_init(char* index_name, struct index_layer* layer, init_func_t init, 
				insert_func_t insert, remove_func_t remove, 
				lookup_func_t lookup, scan_func_t scan, destory_func_t destroy) {
	
	layer->index_struct = init();

	layer->insert = insert;
	layer->remove = remove;
	layer->lookup = lookup;
	layer->scan = scan;
	layer->destory = destroy;

	bonsai_print("index_layer_init: %s\n", index_name);
}

static void index_layer_deinit(struct index_layer* layer) {
	layer->destory(layer->index_struct);

	bonsai_print("index_layer_deinit\n");
}

static int log_layer_init(struct log_layer* layer) {
	int i, err = 0;

	layer->nflush = 0;
	atomic_set(&layer->exit, 0);
	atomic_set(&layer->force_flush, 0);
	atomic_set(&layer->checkpoint, 0);
	atomic_set(&layer->epoch_passed, 0);
    for (i = 0; i < NUM_CPU; i++) {
        atomic_set(&layer->nlogs[i].cnt, 0);
    }
	err = log_region_init(layer, bonsai->desc);
	if (err)
		goto out;

	spin_lock_init(&layer->table_lock);
	INIT_LIST_HEAD(&layer->mptable_list);

    per_node_arena_create(&layer->mptable_arena, "mptable", MPTABLE_ARENA_SIZE, sizeof(struct mptable));

	pthread_barrier_init(&layer->barrier, NULL, NUM_PFLUSH_WORKER);
	for (i = 0; i < NUM_PFLUSH_WORKER; i ++)
		INIT_LIST_HEAD(&layer->sort_list[i]);

	for (i = 0; i < NUM_MERGE_HASH_BUCKET; i ++) {
		INIT_HLIST_HEAD(&layer->buckets[i].head);
		spin_lock_init(&layer->buckets[i].lock);
	}

	bonsai_print("log_layer_init\n");

out:
	return err;
}

static void log_layer_deinit(struct log_layer* layer) {
	struct mptable *m, *table, *tmp;
	int node;

	clean_pflush_buckets(layer);

	log_region_deinit(layer);

	spin_lock(&layer->table_lock);
	list_for_each_entry_safe(table, tmp, &layer->mptable_list, list) {
        for_each_obj(&layer->mptable_arena, node, m, table) {
			hs_destroy(&m->hs);
		}

		list_del(&table->list);
        mptable_free(layer, table);
	}
	spin_unlock(&layer->table_lock);

	bonsai_print("log_layer_deinit\n");
}

static int data_layer_init(struct data_layer* layer) {
	int ret;

	ret = data_region_init(layer);
	if (ret)
		goto out;

	rwlock_init(&layer->lock);
	INIT_LIST_HEAD(&layer->pnode_list);

	bonsai_print("data_layer_init\n");

out:
	return ret;
}

static void data_layer_deinit(struct data_layer* layer) {
	struct pnode* pnode, *tmp;
	int i;

	write_lock(&layer->lock);
	list_for_each_entry_safe(pnode, tmp, &layer->pnode_list, list) {
		list_del(&pnode->list);
	}
	write_unlock(&layer->lock);
	
	data_region_deinit(layer);

	bonsai_print("data_layer_deinit\n");
}

void index_layer_dump() {
	struct index_layer* i_layer = INDEX(bonsai);
	kv_print(i_layer->index_struct);
}

int bonsai_insert(pkey_t key, pval_t value) {
	int cpu = __this->t_cpu, numa_node = get_numa_node(cpu);
	return mptable_insert(numa_node, cpu, key, value);
}

int bonsai_update(pkey_t key, pval_t value) {
	int cpu = __this->t_cpu, numa_node = get_numa_node(cpu);
	return mptable_update(numa_node, cpu, key, &value);
}

int bonsai_remove(pkey_t key) {
	int cpu = __this->t_cpu, numa_node = get_numa_node(cpu);
	return mptable_remove(numa_node, cpu, key);
}

int bonsai_lookup(pkey_t key, pval_t* val) {
	int cpu = __this->t_cpu, numa_node = get_numa_node(cpu);
	return mptable_lookup(numa_node, key, cpu, val);
}

int bonsai_scan(pkey_t low, pkey_t high, pval_t* val_arr) {
	struct mptable *table;
	struct index_layer* i_layer = INDEX(bonsai);
	int arr_size = 0;

	table = (struct mptable*)i_layer->lookup(i_layer->index_struct, low);

	assert(table);

	arr_size = mptable_scan(table, low, high, val_arr);
	
	return arr_size;
}

void bonsai_barrier() {
    atomic_set(&bonsai->l_layer.force_flush, 1);
    do {
        wakeup_master();
        usleep(30000);
    } while (atomic_read(&bonsai->l_layer.force_flush));
    bonsai_print("=== Everything is persistent. ===\n");
}

void bonsai_deinit() {
	bonsai_print("bonsai deinit\n");

	//stat_mptable();
	//dump_pnode_list_summary();
	
	bonsai_self_thread_exit();

	bonsai_pflushd_thread_exit();
	
	index_layer_deinit(&bonsai->i_layer);
    log_layer_deinit(&bonsai->l_layer);
    data_layer_deinit(&bonsai->d_layer);

	munmap((void*)bonsai->desc, PAGE_SIZE);
	close(bonsai->fd);

	free(bonsai);
}

int bonsai_init(char* index_name, init_func_t init, destory_func_t destory,
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
        //index_layer_init(index_name, &bonsai->i_layer, kv_init, 
						//kv_insert, kv_remove, kv_lookup, kv_scan, kv_destory);
		index_layer_init(index_name, &bonsai->i_layer, init, 
						insert, remove, lookup, scan, destory);

		/* 2. initialize log layer */
        error = log_layer_init(&bonsai->l_layer);
		if (error)
			goto out;

		/* 3. initialize data layer */
        error = data_layer_init(&bonsai->d_layer);
		if (error)
			goto out;

		/* 4. initialize self */
		INIT_LIST_HEAD(&bonsai->thread_list);
		bonsai_self_thread_init();
		
		/* 5. initialize sentinel node */
		sentinel_node_init();

		bonsai->desc->init = 1;
    } else {
        bonsai_recover();
    }

	bonsai->desc->epoch = 0;

	/* 6. initialize pflush thread */
	bonsai_pflushd_thread_init();

	bonsai_print("bonsai is initialized successfully!\n");

out:
	return error;
}
