/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@163.com
 */

#include "thread.h"

__thread struct thread_info* __this;

static pthread_mutex_t work_mutex;
static pthread_cond_t work_cond;

static inline void workqueue_add(struct workqueue_struct* wq, struct work_struct* work) {
	list_add(&work->list, &wq->head);
}

static inline void workqueue_del(struct work_struct* work) {
	list_del(&work->list);
	free(work);
}

static void bind_to_cpu(int cpu) {
    int ret;
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((ret = sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        DEBUG("glibc init: bind cpu[%d] failed.\n", cpu);
    }
}

static void init_workqueue(struct thread_info* thread, struct workqueue_struct* wq) {
	wq->thread = thread;
	INIT_LIST_HEAD(&wq->head);
}

void wakeup_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_RUNNING);
	pthread_cond_broadcast(&work_cond);
	pthread_mutex_unlock(&work_mutex);
}

static void try_wakeup_master() {
	if (atomic_sub_return(1, &STATUS) == WORKER_SLEEP) {
		pthread_mutex_lock(&work_mutex);
		pthread_cond_broadcast(&work_cond);
		pthread_mutex_unlock(&work_mutex);
	}
}

void park_workers() {
	pthread_mutex_lock(&work_mutex);
	atomic_set(&STATUS, WORKER_SLEEP);
	pthread_mutex_unlock(&work_mutex);
}

void park_master() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_SLEEP) 
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void worker_sleep() {
	pthread_mutex_lock(&work_mutex);
	while (atomic_read(&STATUS) != WORKER_RUNNING)
		pthread_cond_wait(&work_cond, &work_mutex);
	pthread_mutex_unlock(&work_mutex);
}

static void thread_work(struct workqueue_struct* wq) {
	struct work_struct* work, *tmp;

	printf("worker thread[%d] start to work\n", __this->thread_id);

	if (likely(list_empty(&wq->head)))
		return;

	list_for_each_entry_safe(work, tmp, &wq->head, list) {
		work->func(work->arg);
		workqueue_del(work);
	}

	try_wakeup_master();
}

static void pflush_worker(struct thread_info* this) {
	__this = this;

	printf("pflush thread[%d] start.\n", __this->thread_id);

	bind_to_cpu(__this->thread_cpu);
	
	while (1) {
		worker_sleep();
		
		thread_work(&__this->workqueue);
	}
}

static void pflush_master(struct thread_info* this) {
	__this = this;

	printf("pflush thread[%d] start.\n", __this->thread_id);

	bind_to_cpu(__this->thread_cpu);

	long int i = 0;
	for (;;) {		
		sleep(5);
		if (i == 0)
			gc_master_work(__this);

		printf("master gc thread[%d] finish mark-and-compact gc\n", __this->thread_id);
		i++;
		sleep(100);
	}
}

int bonsai_thread_init(struct bonsai* bonsai) {
	struct thread_info* thread;
	int ret;

	pthread_mutex_init(&work_mutex, NULL);
	pthread_cond_init(&work_cond, NULL);

	for (int i = 0; i < NUM_PFLUSH_THREAD; i++) {
		thread = malloc(sizeof(struct thread_info));
		thread->t_id = i;
		thread->t_cpu = i;

		init_workqueue(thread, &thread->t_wq);

		thread->t_data = NULL;

		if (pthread_create(&bonsai->tids[i], NULL, 
			(i == 0) ? (void*)pflush_master : (void*)pflush_worker, (void*)thread) != 0) {
        		printf("bonsai create thread[%d] failed\n", thread->t_id);
			return -ERR_THREAD;
    	}

		bonsai->pflushd[i] = thread;
	}
	
	return 0;
}

int bonsai_thread_exit(struct bonsai* bonsai) {
	int i;

	for (i = 0; i < NUM_PFLUSH_THREAD; i++) {
		free(bonsai->pflushd[i]);
	}
	
	return 0;
}
