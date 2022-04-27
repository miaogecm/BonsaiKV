/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 		   Junru Shen, gnu_emacs@hhu.edu.cn
 *
 * Bonsai thread configuration:
 *
 * self | master | pflush | pflush | pflush | pflush | smo
 *                      node0              node1
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>

#include "thread.h"
#include "cpu.h"
#include "atomic.h"
#include "common.h"
#include "bonsai.h"
#include "log_layer.h"
#include "index_layer.h"

__thread struct thread_info* __this = NULL;

static pthread_mutex_t work_mutex;
static pthread_cond_t work_cond;

static atomic_t STATUS = ATOMIC_INIT(MASTER_SLEEP);
static atomic_t RECEIVED = ATOMIC_INIT(0);

static atomic_t tids = ATOMIC_INIT(-1);

static pthread_mutex_t smo_mutex;
static pthread_cond_t smo_cond;

static atomic_t SMO_STATUS = ATOMIC_INIT(SMO_SLEEP);

void do_smo();

extern struct bonsai_info* bonsai;

inline int get_tid() {
	return __this->t_id;
}

static void init_workqueue(struct thread_info* thread, struct workqueue_struct* wq) {
	wq->thread = thread;
	INIT_LIST_HEAD(&wq->head);
}

void wakeup_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_RUNNING);
	atomic_set(&RECEIVED, 0);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

static void try_wakeup_master() {
	while (atomic_read(&RECEIVED) != WORKER_RUNNING) {
        cpu_relax();
    }
	if (atomic_sub_return(1, &STATUS) == WORKER_SLEEP) {
		pthread_mutex_lock(&work_mutex);
		pthread_cond_broadcast(&work_cond);
		pthread_mutex_unlock(&work_mutex);
	}
}

void wakeup_master() {
	if (atomic_read(&STATUS) != MASTER_SLEEP)
		return;
	
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, MASTER_WORK);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

void park_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_SLEEP);
	pthread_mutex_unlock(&work_mutex);
}

void park_master() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_SLEEP 
			&& atomic_read(&STATUS) != ALL_WAKEUP
			&& atomic_read(&STATUS) != MASTER_WORK)
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void worker_sleep() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_RUNNING 
			&& atomic_read(&STATUS) != ALL_WAKEUP) {
		pthread_cond_wait(&work_cond, &work_mutex);
	}
	atomic_inc(&RECEIVED);
	pthread_mutex_unlock(&work_mutex);
}

static void wakeup_all() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, ALL_WAKEUP);
	atomic_set(&RECEIVED, 0);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

static void wait_pflush_thread() {
	int states[NUM_PFLUSH_THREAD] = {0};
	int i, num_sleep = 0;

	while (1) {
		for (i = 0; i < NUM_PFLUSH_THREAD; i ++) {
			if (bonsai->pflush_threads[i]->t_state != S_SLEEPING)
				usleep(20);
			else
				states[i] = 1;
		}

		for (i = 0; i < NUM_PFLUSH_THREAD; i ++) {
			if (states[i]) num_sleep ++;
		}

		if (num_sleep == NUM_PFLUSH_THREAD)
			return;

		num_sleep = 0;
	}
}

static void thread_work(struct workqueue_struct* wq) {
	struct work_struct* work, *tmp;
	int ret = 0;

	bonsai_debug("worker thread[%d] start to work\n", __this->t_id);

	if (likely(list_empty(&wq->head)))
		return;

	list_for_each_entry_safe(work, tmp, &wq->head, list) {
		__this->t_work = work;
		ret = work->exec(work->exec_arg);
		workqueue_del(work);

		if (unlikely(ret)) 
			return;
	}

	try_wakeup_master();
}

static void pflush_thread_exit(struct thread_info* thread) {
	struct log_layer *layer = LOG(bonsai);

	list_del(&thread->list);
	thread->t_state = S_EXIT;

	atomic_inc(&layer->exit);

	spin_lock(&bonsai->list_lock);
	list_del(&thread->list);
	spin_unlock(&bonsai->list_lock);

	bonsai_print("pflush thread[%d] exit\n", thread->t_id);
}

static void pflush_worker(struct thread_info* this) {
	struct log_layer *layer = LOG(bonsai);

	__this = this;
	__this->t_pid = gettid();
	__this->t_state = S_RUNNING;

	bind_to_cpu(__this->t_cpu);

	bonsai_print("pflush worker thread[%d] pid[%d] start on cpu[%d]\n", 
			__this->t_id, __this->t_pid, get_cpu());
	
	while (!atomic_read(&layer->exit)) {
		__this->t_state = S_SLEEPING;
		worker_sleep();
		
		__this->t_state = S_RUNNING;
		thread_work(&__this->t_wq);
	}

	pflush_thread_exit(this);
}

static void master_wait_workers(struct thread_info* master) {
	struct thread_info* thread;
	int i, num_worker = 0;

	while (1) {
		for (i = 1; i < NUM_PFLUSH_THREAD; i ++) {
			thread = bonsai->pflush_threads[i];

            if (thread->t_state == S_UNINIT) {
                usleep(10);
                break;
            }
            else
                num_worker ++;

            if (num_worker == NUM_PFLUSH_WORKER)
                return;
		}
	}
}

static inline int need_flush_log() {
	struct log_layer *layer = LOG(bonsai);
    int cpu, total = 0;
    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        total += atomic_read(&layer->nlogs[cpu].cnt);
        if (total >= CHKPT_NLOG_INTERVAL) {
            return 1;
        }
    }
    return 0;
}

static void pflush_master(struct thread_info* this) {
	struct log_layer *layer = LOG(bonsai);
	
	__this = this;
	__this->t_pid = gettid();
	__this->t_state = S_RUNNING;

	bind_to_cpu(__this->t_cpu);
	
	bonsai_print("pflush master thread[%d] pid[%d] start on cpu[%d]\n", 
		__this->t_id, __this->t_pid, get_cpu());

	master_wait_workers(this);

	while (!atomic_read(&layer->exit)) {
		__this->t_state = S_SLEEPING;
		atomic_set(&STATUS, MASTER_SLEEP);
		park_master();

		__this->t_state = S_RUNNING;
		while(need_flush_log() && !atomic_read(&layer->exit)) {
			oplog_flush(bonsai);
        }

        if (unlikely(atomic_read(&layer->force_flush))) {
            oplog_flush(bonsai);
            atomic_set(&layer->force_flush, 0);
        }
	}

	pflush_thread_exit(this);
}

void wakeup_smo() {
	if (atomic_read(&SMO_STATUS) != SMO_SLEEP) {
		return;
	}

	pthread_mutex_lock(&smo_mutex);
	atomic_set(&SMO_STATUS, SMO_WORK);
	pthread_cond_broadcast(&smo_cond);
	pthread_mutex_unlock(&smo_mutex);
}

static void park_smo() {
	pthread_mutex_lock(&smo_mutex);
	while(atomic_read(&SMO_STATUS) != SMO_WORK) {
		pthread_cond_wait(&smo_cond, &smo_mutex);
	}
	pthread_mutex_unlock(&smo_mutex);
}

static void smo_thread_exit(struct thread_info* thread) {
	struct shim_layer *layer = SHIM(bonsai);

	list_del(&thread->list);
	thread->t_state = S_EXIT;

	atomic_inc(&layer->exit);
	
	bonsai_print("smo thread[%d] exit\n", thread->t_id);
}

static void smo_worker(struct thread_info* this) {
	struct shim_layer* layer = SHIM(bonsai);

    __this = this;

	this->t_pid = gettid();
	this->t_state = S_RUNNING;

	bind_to_cpu(this->t_cpu);

	bonsai_print("smo thread[%d] pid[%d] start on cpu[%d]\n",
                 this->t_id, this->t_pid, get_cpu());

	while(!atomic_read(&layer->exit)) {
		this->t_state = S_SLEEPING;
		atomic_set(&SMO_STATUS, SMO_SLEEP);
		park_smo();

		this->t_state = S_RUNNING;
		if (!atomic_read(&layer->exit)) {
			do_smo();
		}
	}

	smo_thread_exit(this);
}

void bonsai_self_thread_init() {
	
	bonsai->self = malloc(sizeof(struct thread_info));
	bonsai->self->t_id = atomic_add_return(1, &tids);
	bonsai->self->t_pid = gettid();
	bonsai->self->t_state = S_RUNNING;

	__this = bonsai->self;

	spin_lock(&bonsai->list_lock);
	list_add(&__this->list, &bonsai->thread_list);
	spin_unlock(&bonsai->list_lock);

	bonsai_print("self thread[%d] pid[%d] start on cpu[%d]\n", __this->t_id, __this->t_pid, get_cpu());
}

void bonsai_self_thread_exit() {
	struct thread_info* thread = bonsai->self;

	spin_lock(&bonsai->list_lock);
	list_del(&thread->list);
	spin_unlock(&bonsai->list_lock);
	
	free(thread);
}

int bonsai_user_thread_init(pthread_t tid) {
	struct thread_info* thread;

	thread = malloc(sizeof(struct thread_info));
	thread->t_id = atomic_add_return(1, &tids);
	thread->t_pid = gettid();
  	thread->t_cpu = get_cpu();
	thread->t_state = S_RUNNING;
	thread->t_epoch = bonsai->desc->epoch;
	thread->t_stm = stm_init_thread();

	spin_lock(&bonsai->list_lock);
  	list_add(&thread->list, &bonsai->thread_list);
	spin_unlock(&bonsai->list_lock);

	bonsai->tids[thread->t_id] = tid;
	bonsai->user_threads[thread->t_id - NUM_PFLUSH_THREAD - NUM_SMO_THREAD - 1] = thread;

	/* software transactional memory */
	stm_init_thread();

	/* quiescent state based reclamation */
	fb_set_tid(thread->t_id);
	rcu_thread_online(RCU(bonsai));

	__this = thread;

	bonsai_print("user thread[%d] pid[%d] start on cpu[%d]\n", __this->t_id, __this->t_pid, get_cpu());

	return 0;
}

void bonsai_user_thread_exit() {
	struct thread_info* thread = __this;

	spin_lock(&bonsai->list_lock);
	list_del(&thread->list);
	spin_unlock(&bonsai->list_lock);

	stm_exit_thread(thread->t_stm);
	
	rcu_thread_offline(RCU(bonsai));

	free(thread);
}

int bonsai_pflushd_thread_init() {
	struct thread_info* thread;
	int node, i;

	pthread_mutex_init(&work_mutex, NULL);
	pthread_cond_init(&work_cond, NULL);

    /* init master on node 0 */
    thread = malloc(sizeof(struct thread_info));
    thread->t_id = atomic_add_return(1, &tids);
    thread->t_cpu = alloc_cpu_onnode(0);
    thread->t_state = S_UNINIT;
    init_workqueue(thread, &thread->t_wq);
    thread->t_data = NULL;
	spin_lock(&bonsai->list_lock);
    list_add(&thread->list, &bonsai->thread_list);
	spin_unlock(&bonsai->list_lock);
    bonsai->pflush_master = thread;

    /* init workers */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            thread = malloc(sizeof(struct thread_info));
            thread->t_id = atomic_add_return(1, &tids);
            thread->t_cpu = alloc_cpu_onnode(node);
            thread->t_state = S_UNINIT;
            init_workqueue(thread, &thread->t_wq);
            thread->t_data = NULL;
			spin_lock(&bonsai->list_lock);
            list_add(&thread->list, &bonsai->thread_list);
			spin_unlock(&bonsai->list_lock);
            bonsai->pflush_workers[node][i] = thread;
        }
    }

    /* create master thread */
    if (pthread_create(&bonsai->tids[bonsai->pflush_master->t_id], NULL, (void *) pflush_master, bonsai->pflush_master) != 0) {
        perror("bonsai create master thread failed");
        return -ETHREAD;
    }
    pthread_setname_np(bonsai->tids[bonsai->pflush_master->t_id], "pflush_master");

    /* create worker threads */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            if (pthread_create(&bonsai->tids[bonsai->pflush_workers[node][i]->t_id], NULL, (void *) pflush_worker, bonsai->pflush_workers[node][i]) != 0) {
                perror("bonsai create worker thread failed");
                return -ETHREAD;
            }
            pthread_setname_np(bonsai->tids[bonsai->pflush_workers[node][i]->t_id], "pflush_worker");
        }
    }

    /* wait for master and workers to sleep */
	wait_pflush_thread();
	
	return 0;
}

int bonsai_smo_thread_init() {
	struct thread_info* thread;

    pthread_mutex_init(&smo_mutex, NULL);
	pthread_cond_init(&smo_cond, NULL);

	thread = malloc(sizeof(struct thread_info));
	thread->t_id = atomic_add_return(1, &tids);
	thread->t_cpu = alloc_cpu_onnode(0);
	thread->t_state = S_UNINIT;
	init_workqueue(thread, &thread->t_wq);
	thread->t_data = NULL;

  	INIT_LIST_HEAD(&thread->list);
	spin_lock(&bonsai->list_lock);
	list_add(&thread->list, &bonsai->thread_list);
	spin_unlock(&bonsai->list_lock);

	bonsai->smo = thread;

	if (pthread_create(&bonsai->tids[NUM_PFLUSH_THREAD + 1], NULL,
		(void*)smo_worker, (void*)bonsai->smo) != 0) {
			perror("bonsai create thread failed\n");
		return -ETHREAD;
	}
    pthread_setname_np(bonsai->tids[NUM_PFLUSH_THREAD + 1], "smo_worker");
	
	return 0;
}

int bonsai_smo_thread_exit() {
	struct shim_layer* layer = SHIM(bonsai);

    atomic_set(&layer->exit, 1);

	while (atomic_read(&layer->exit) != 2) {
		wakeup_smo();
		usleep(10);
	}

	pthread_join(bonsai->tids[NUM_PFLUSH_THREAD], NULL);
	spin_lock(&bonsai->list_lock);
	list_del(&bonsai->smo->list);
	spin_unlock(&bonsai->list_lock);
	free(bonsai->smo);

	bonsai_print("smo thread exit\n");

	return 0;
}

int bonsai_pflushd_thread_exit() {
	struct log_layer* layer = LOG(bonsai);
	int i;

	atomic_set(&layer->exit, 1);

	while (atomic_read(&layer->exit) != (NUM_PFLUSH_THREAD + 1)) {
		wakeup_all();
		usleep(10);
	}

	for (i = 0; i < NUM_PFLUSH_THREAD + 1; i++) {
		pthread_join(bonsai->tids[i], NULL);
		free(bonsai->pflush_threads[i]);
	}

	bonsai_print("pflush thread exit\n");
	
	return 0;
}