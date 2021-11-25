#ifndef __THREAD_H
#define __THREAD_H

#include <pthread.h>

#define NUM_PFLUSH_THREAD

typedef void (*work_func_t)(void*);

struct work_struct {
	work_func_t func;
	void* arg;
	struct list_head list;
};

struct workqueue_struct {
	struct thread_info* thread;
	struct list_head head;
};

struct thread_info {
	unsigned int 	t_id;
	unsigned int 	t_cpu;
	void* 			t_data;
	struct workqueue_struct t_wq;		
};

extern __thread struct thread_info* __this;

extern int bonsai_thread_init(struct bonsai* bonsai);
extern int bonsai_thread_exit(struct bonsai* bonsai);

#endif
