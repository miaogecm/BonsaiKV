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
#include "config.h"
#include "index_layer.h"
#include "log_layer.h"
#include "data_layer.h"
#include "rcu.h"
#include "counter.h"

struct bonsai_info* bonsai;
static char* bonsai_fpath = "/mnt/ext4/dimm0/bonsai";

int cpu_used[NUM_CPU] = { 0 };

extern void* index_struct(void* index_struct);
extern void kv_print(void* index_struct);

#ifdef STR_KEY

pkey_t bonsai_make_key(const void *key, size_t len) {
    pkey_t res = { 0 };
    memcpy(res.key, key, len);
    return res;
}

#endif

#define OUTSIDE_DTX      (-1)

__thread int op_count = 0;
__thread log_state_t dtx_lst = { .flip = OUTSIDE_DTX };

static inline void try_quiescent() {
    if (op_count > RCU_MAX_OP) {
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

void bonsai_dtx_start() {
    /* Do not support nested durable transaction. */
    assert(dtx_lst.flip == OUTSIDE_DTX);
  	oplog_snapshot_lst(&dtx_lst);
}

static void leave_dtx() {
    assert(dtx_lst.flip != OUTSIDE_DTX);
    try_quiescent();
    dtx_lst.flip = OUTSIDE_DTX;
}

void bonsai_dtx_rollback() {
    assert(dtx_lst.flip != OUTSIDE_DTX);
    oplog_insert(&dtx_lst, MIN_KEY, 0, OP_NOP, TX_ROLLBACK, __this->t_cpu);
    leave_dtx();
}

void bonsai_dtx_commit() {
    assert(dtx_lst.flip != OUTSIDE_DTX);
    oplog_insert(&dtx_lst, MIN_KEY, 0, OP_NOP, TX_COMMIT, __this->t_cpu);
    leave_dtx();
}

static void check_dtx_autostart() {
    if (dtx_lst.flip == OUTSIDE_DTX) {
        bonsai_dtx_start();
    }
}

static int do_bonsai_insert(pkey_t key, pval_t value, txop_t txop) {
  	logid_t log;
	int ret;

    check_dtx_autostart();

    log = oplog_insert(&dtx_lst, key, valman_make_nv(value), OP_INSERT, txop, __this->t_cpu);

    ret = shim_upsert(&dtx_lst, key, log);

    op_count++;
    if (txop != TX_OP) {
        leave_dtx();
    }

    return ret;
}

static int do_bonsai_remove(pkey_t key, txop_t txop) {
    logid_t log;
    int ret;

    check_dtx_autostart();

    log = oplog_insert(&dtx_lst, key, 0, OP_REMOVE, txop, __this->t_cpu);

    ret = shim_upsert(&dtx_lst, key, log);

    op_count++;
    if (txop != TX_OP) {
        leave_dtx();
    }

    return ret;
}

int bonsai_insert(pkey_t key, pval_t value) {
    return do_bonsai_insert(key, value, TX_OP);
}

int bonsai_insert_commit(pkey_t key, pval_t value) {
    return do_bonsai_insert(key, value, TX_COMMIT);
}

int bonsai_remove(pkey_t key) {
    return do_bonsai_remove(key, TX_OP);
}

int bonsai_remove_commit(pkey_t key) {
    return do_bonsai_remove(key, TX_COMMIT);
}

int bonsai_lookup(pkey_t key, pval_t *val) {
    pval_t nv_val;
    int ret;

    assert(dtx_lst.flip == OUTSIDE_DTX);

    ret = shim_lookup(key, &nv_val);

    if (likely(!ret)) {
        *val = valman_make_v_local(nv_val);
    }

    op_count++;
    try_quiescent();

    return ret;
}

int bonsai_scan(pkey_t start, int range, pval_t *values) {
    assert(dtx_lst.flip == OUTSIDE_DTX);

	shim_scan(start, range, values);

    op_count++;
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
#ifdef ASYNC_SMO
  bonsai_smo_thread_exit();
#endif
	
	index_layer_deinit(&bonsai->i_layer);
  log_layer_deinit(&bonsai->l_layer);
  data_layer_deinit(&bonsai->d_layer);

	munmap((void*)bonsai->desc, ALIGN(sizeof(struct bonsai_desc), PMM_PAGE_SIZE));
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

#ifndef USE_DEVDAX
	if ((error = posix_fallocate(fd, 0, PAGE_SIZE)) != 0) {
		perror("posix_fallocate");
		goto out;
	}
#endif

	addr = mmap(NULL, ALIGN(sizeof(struct bonsai_desc), PMM_PAGE_SIZE), PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FILE, fd, 0);
	if (addr == MAP_FAILED) {
		perror("mmap");
		error = -EMMAP;
		goto out;
	}

  	bonsai = malloc(sizeof(struct bonsai_info));

	bonsai->fd = fd;
	bonsai->desc = (struct bonsai_desc*)addr;

    /* TODO: Recovery */
    bonsai->desc->init = 0;

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

#ifdef ASYNC_SMO
	bonsai_smo_thread_init();
#endif

	bonsai_print("bonsai is initialized successfully!\n");

out:
	return error;
}

size_t get_inode_size();
size_t get_cnode_size();

size_t bonsai_get_dram_usage() {
    int nr_ino = COUNTER_GET(nr_ino), nr_pno = COUNTER_GET(nr_pno);
    size_t index_mem = COUNTER_GET(index_mem), mem;

    mem = index_mem + nr_ino * get_inode_size();
#ifndef DISABLE_UPLOAD
    mem += nr_pno * get_cnode_size();
#else
    (void) nr_pno;
#endif

    return mem;
}
