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

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

static struct oplog* alloc_oplog(pkey_t key, pval_t val, void* addr, optype_t type) {
	struct oplog* log = malloc(sizeof(struct oplog));

	log->o_stamp = ordo_new_clock(0);
	log->o_type = type;
	log->o_addr = addr;
	log->o_kv.key = key;
	log->o_kv.val = (pkey_t) val;
	INIT_LIST_HEAD(&log->o_list);
}

struct oplog* oplog_insert(pkey_t key, pval_t val, void* addr, optype_t op) {
	int cpu = get_cpu();
	struct list_head *head;
	struct oplog* log;
	
	log = alloc_oplog(key, val, addr, op);
	head = &LOG(bonsai).oplogs[cpu];
	
	spin_lock(&LOG(bonsai).locks[cpu]);
	list_add_tail(&log->list, head);
	spin_unlock(&LOG(bonsai).locks[cpu]);

	atomic_add(&LOG(bonsai).num_log[cpu]);
}

static void worker_oplog_flush(void* arg) {
	struct flush_work* fwork = (struct flush_work*)arg;
	struct oplog* log = fwork->from;
	int ret;

	list_for_each_entry_from(log, fwork->head, list) {
		switch(log->o_type) {
		case OP_INSERT:
		ret = pnode_insert(log->o_node, log->o_kv.key, log->o_kv.val);
		break;
		case OP_REMOVE:
		ret = pnode_remove(log->o_node, log->o_);
		break;
		default:
		printf("bad operation type\n");
		break;
		}
	}

	free(fwork);
}

#ifdef BONSAI_PAR_LOG_SORT
static int oplog_cmp(void *priv, struct list_head *a, struct list_head *b) {
	struct oplog* x = list_entry(a, struct oplog, list);
	struct oplog* y = list_entry(b, struct oplog, list);

	if (ordo_cmp_clock(x->o_stamp, y->o_stamp)) return 1;
	else if (ordo_cmp_clock(x->o_stamp, y->o_stamp)) return -1;
	else return 0;
}

static void oplog_list_sort(struct list_head* head) {
	list_sort(NULL, head, oplog_cmp);
}

static void copy_list(struct list_head* dst, struct list_head* src) {
	struct oplog *d, *tmp, *n;

	list_for_each_entry(n, src, list) {
		d = malloc(sizeof(struct data));
		*d = *n;
		
		list_add_tail(&d->list, dst);
	}
}

static void free_second_half_list(struct list_head* head) {
	struct list_head* pos;
	struct oplog *d, *tmp;
	int sum = 0, half, i = 0;

	list_for_each(pos, head) {
		sum ++;
	}

	half = sum / 2;
	list_for_each_entry_safe_reverse(d, tmp, head, list) {
		if (i ++ < half) {
			list_del(&d->list);
			free(d);
		} else {
			break;
		}
	}
}

static void free_first_half_list(struct list_head* head) {
	struct list_head* pos;
	struct oplog *d, *tmp;
	int sum = 0, half, i = 0;

	list_for_each(pos, head) {
		sum ++;
	}

	half = sum / 2;
	list_for_each_entry_safe(d, tmp, head, list) {
		if (i ++ < half) {
			list_del(&d->list);
			free(d);
		} else {
			break;
		}
	}
}

pthread_barrier_t barrier;

/*
 * worker_oplog_sort: parallel sorting based on odd–even sort algorithm
 */
static void worker_oplog_sort(void *arg) {
	struct sort_work *swork = (struct sort_work*)arg;
	struct list_head **heads = swork->heads;
	int i, round = NUM_PFLUSH_THREAD;
	int id = swork->id;

	oplog_list_sort(heads[id]);

	pthread_barrier_wait(&barrier);

	for (i = 0; i < round; i ++) {
		if (i % 2 == 0) {
			if (id % 2 == 0) {
				list_splice(heads[id], heads[id + 1]);
				INIT_LIST_HEAD(heads[id]);
				copy_list(heads[id], heads[id + 1]);
				pthread_barrier_wait(&barrier);
				
				oplog_list_sort(heads[id]);
				free_second_half_list(heads[id]);
				pthread_barrier_wait(barrier);	
			} else {
				pthread_barrier_wait(&barrier);
				oplog_list_sort(heads[id]);
				free_first_half_list(heads[id]);
				pthread_barrier_wait(&barrier);	
			}
		} else {
			if (id % 2 == 0) {
				pthread_barrier_wait(&barrier);
				if (id != 0) {
					oplog_list_sort(heads[id]);
					free_first_half_list(heads[id]);
				}
				pthread_barrier_wait(&barrier);	
			} else {
				if (id != NUM_THREAD - 1) {
					list_splice(heads[id], heads[id + 1]);
					INIT_LIST_HEAD(heads[id]);
					copy_list(heads[id], heads[id + 1]);
				}

				pthread_barrier_wait(&barrier);
				if (id != NUM_THREAD - 1) {
					oplog_list_sort(heads[id]);
					free_second_half_list(heads[id]);
				}
				pthread_barrier_wait(&barrier);
			}
		}
	}

	free(swork);
}
#endif

void oplog_flush(struct bonsai* bonsai) {
	struct log_layer *layer = LOG(bonsai);
	struct list_head _heads[NUM_CPU], **heads;
	struct list_head *pos, *log_list;
	struct oplog *log, *tmp, *pos;
	struct flush_work* fworks[NUM_PFLUSH_THREAD];
	struct work_struct* work;
	struct sort_work *swork;
	int cpu, cnt, i, total_log = 0;
	int num_log_per_worker;
	
	/* 1. fetch and merge all log lists */
	for (cpu = 0; cpu < bonsai->total_cpu; cpu ++) {
		spin_lock(&layer->locks[cpu]);
		_heads[cpu] = layer->log_list[cpu];
		INIT_LIST_HEAD(&layer->log_list[cpu]);
		total_log += atomic_read(&LOG(bonsai).num_log[cpu]);
		atomic_set(&LOG(bonsai).num_log[cpu], 0)
		spin_unlock(&layer->locks[cpu]);
#ifndef BONSAI_PAR_LOG_SORT
		list_splice(&_heads[cpu], &log_list);
#endif
	}

	/* 2. sort all logs */
	log_list = malloc(sizeof(struct list_head));
	INIT_LIST_HEAD(log_list);
#ifdef BONSAI_PAR_LOG_SORT
	heads = malloc(NUM_PFLUSH_THREAD * sizeof(void*))
	for (i = 0; i < NUM_PFLUSH_THREAD; i ++) {
		heads[i] = malloc(sizeof(struct list_head));
		INIT_LIST_HEAD(heads[i]);	
	}

	i = 0;
	for (cpu = 0; cpu < bonsai->total_cpu; cpu ++) {
		list_splice(&_heads[cpu], heads[i++ % NUM_PFLUSH_THREAD]);
	}

	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		swork = malloc(sizeof(struct sort_work));
		swork->heads = heads;
		swork->id = cnt;

		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_sort;
		work->arg = (void*)(swork);
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}

	wakeup_workers();

	park_master();
	
	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		list_splice(heads[cnt], log_list);
		free(heads[cnt]);
	}
#else
	oplog_list_sort(log_list);
#endif

	/* 3. dispatch works to threads */
	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		fworks[cnt] = malloc(sizeof(struct flush_work));
		fworks[cnt]->id = cnt;
		fworks[cnt]->head = log_list;
	}	

	i = cnt = 0; num_log_per_worker = total_log / NUM_PFLUSH_THREAD;
	list_for_each(pos, &log_list) {
		if (i % num_log_per_worker == 0) {
			fworks[cnt++]->from = list_entry(pos, struct oplog, o_list);
		}
		if (cnt == num_log_per_worker - 1)
			break;
	}
	fworks[cnt++]->from = list_entry(pos, struct oplog, o_list);	

	for (cnt = 0; cnt < NUM_PFLUSH_THREAD; cnt ++) {
		work = malloc(sizeof(struct work_struct));
		INIT_LIST_HEAD(&work->list);
		work->func = worker_oplog_flush;
		work->arg = (void*)(fworks[cnt]);
		workqueue_add(&bonsai->pflushd[cnt + 1]->workqueue, work);
	}
	
	wakeup_workers();

	park_master();

	/* 4. commit all changes */
	list_for_each_entry_safe(log, tmp, &head, list) {
		commit_bitmap(log->o_node, , log->o_type);
		list_del(&log->o_list);
		free(log);
	}
}
