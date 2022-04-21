/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
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
#include "hwconfig.h"
#include "index_layer.h"
#include "data_layer.h"
#include "cpu.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/node0/bonsai";

int cpu_used[NUM_CPU] = { 0 };

extern void* index_struct(void* index_struct);
extern void kv_print(void* index_struct);

#ifdef LONG_KEY
pkey_t bonsai_make_key(const void *key, size_t len) {
    return pkey_generate_v(key, len);
}
#endif

void bonsai_mark_cpu(int cpu) {
    mark_cpu(cpu);
}

int bonsai_insert(pkey_t key, pval_t value) {
	int cpu = __this->t_cpu, numa_node = get_numa_node(cpu);
    logid_t log = oplog_insert(key, value, OP_INSERT, cpu);
    log_state_t snap;
    oplog_snapshot_lst(&snap);
	return shim_upsert(&snap, key, log);
}

int bonsai_remove(pkey_t key) {
}

int bonsai_lookup(pkey_t key, pval_t *val) {
    return shim_lookup(key, val);
}

int bonsai_scan(pkey_t low, uint16_t lo_len, pkey_t high, uint16_t hi_len, pval_t* val_arr) {
}

void bonsai_barrier() {
    atomic_set(&bonsai->l_layer.force_flush, 1);
    do {
        wakeup_master();
        usleep(30000);
    } while (atomic_read(&bonsai->l_layer.force_flush));
    printf("=== Everything is persistent. ===\n");
}

void bonsai_deinit() {
	bonsai_print("bonsai deinit\n");

	//stat_mptable();
	//dump_pnode_list_summary();
	
	bonsai_self_thread_exit();

	bonsai_pflushd_thread_exit();
    bonsai_smo_thread_exit();
	
	index_layer_deinit(&bonsai->i_layer);
    log_layer_deinit(&bonsai->l_layer);
    data_layer_deinit(&bonsai->d_layer);

	munmap((void*)bonsai->desc, PAGE_SIZE);
	close(bonsai->fd);

	free(bonsai);
}

int bonsai_init(char *index_name, init_func_t init, destory_func_t destory, insert_func_t insert, update_func_t update,
                remove_func_t remove, lookup_func_t lookup, scan_func_t scan) {
	int error = 0, fd, node;
    pnoid_t sentinel;
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
		index_layer_init(index_name, &bonsai->i_layer, init, 
						 insert, update, remove, lookup, scan, destory);

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
		
		/* 5. initialize sentinel nodes */
        sentinel = pnode_sentinel_init();
        shim_sentinel_init(sentinel);

		bonsai->desc->init = 1;
    } else {
        bonsai_recover();
    }

	bonsai->desc->epoch = 0;

	/* 6. initialize pflush thread */
	bonsai_pflushd_thread_init();
	bonsai_smo_thread_init();

	bonsai_print("bonsai is initialized successfully!\n");

out:
	return error;
}
