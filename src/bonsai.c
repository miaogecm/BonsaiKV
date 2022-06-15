/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include "cpu.h"

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
#include "rcu.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/dimm0/bonsai";

int cpu_used[NUM_CPU] = { 0 };

extern void* index_struct(void* index_struct);
extern void kv_print(void* index_struct);

#ifdef STR_KEY

pkey_t bonsai_make_key(const void *key, size_t len) {
    return pkey_generate_v(key, len);
}

#endif

__thread int op_count = 0;

static inline void try_quiescent() {
    if (op_count ++ > RCU_MAX_OP) {
        op_count = 0;
        rcu_quiescent(RCU(bonsai));
    }
}

void bonsai_mark_cpu(int cpu) {
    mark_cpu(cpu);
}

void bonsai_online() {
	rcu_thread_online(RCU(bonsai));
}

void bonsai_offline() {
    rcu_thread_offline(RCU(bonsai));
}

int bonsai_insert(pkey_t key, pval_t value) {
  	log_state_t snap;
  	logid_t log;
	int ret;
	
  	oplog_snapshot_lst(&snap);

    log = oplog_insert(&snap, key, valman_make_nv(value), OP_INSERT, __this->t_cpu);

    ret = shim_upsert(&snap, key, log);

    try_quiescent();

    return ret;
}

int bonsai_remove(pkey_t key) {
    log_state_t snap;
    logid_t log;
    int ret;
		
  	oplog_snapshot_lst(&snap);

    log = oplog_insert(&snap, key, 0, OP_REMOVE, __this->t_cpu);

    ret = shim_upsert(&snap, key, log);

    try_quiescent();

    return ret;
}

int bonsai_lookup(pkey_t key, pval_t *val) {
    pval_t nv_val;
    int ret;

    ret = shim_lookup(key, &nv_val);

    if (likely(!ret)) {
        //*val = valman_make_v_local(nv_val);
        *val = valman_make_v(nv_val);
    }

    try_quiescent();

    return ret;
}

struct wrapper_argv {
	scanner_t scanner;
	void* argv;
};

static scanner_ctl_t wrapper(pentry_t e, void* w_argv_) {
	struct wrapper_argv* w_argv = (struct wrapper_argv*)w_argv_;
	scanner_t scanner = w_argv->scanner;

	e.v = valman_make_v(e.v);
	//TODO: free it

	return scanner(e, w_argv->argv);
}

int bonsai_scan(pkey_t start, scanner_t scanner, void* argv) {
	struct wrapper_argv w_argv = {scanner, argv};

	shim_scan(start, wrapper, &w_argv);

	try_quiescent();

	return 0;
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
	int error = 0, fd;
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

		/* 6. initialize RCU */
		rcu_init(&bonsai->rcu);
		fb_init(&bonsai->rcu.fb);

		/* 7. initialize durable transaction */
		atomic_set(&bonsai->tx_id, 0);

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