#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sched.h>
#include "list.h"

#define MAX     100000	
#define MIN	0

#define N 100

#define NUM_THREAD		4
struct list_head heads[NUM_THREAD];
pthread_t tids[NUM_THREAD];

pthread_barrier_t barrier;

struct data {
	int i;
	struct list_head list;
};

extern void list_sort(void *priv, struct list_head *head,
		int (*cmp)(void *priv, struct list_head *a,
			struct list_head *b));

static inline int __random() {
	return rand() % MAX + MIN;
}

static void bind_to_cpu(int cpu) {
    int ret;
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((ret = sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        printf("glibc init: bind cpu[%d] failed.\n", cpu);
    }
}

static void list_init() {
	struct data *data;
	int i, j;

	srand((unsigned)time(NULL));

	for (i = 0; i < NUM_THREAD; i ++) {
		INIT_LIST_HEAD(&heads[i]);
		for (j = 0; j < N; j ++) {
			data = malloc(sizeof(struct data));
			data->i = __random();
			list_add(&data->list, &heads[i]);
		}
		printf("list [%d] init\n", i);
	}

}

static int cmp(void *priv, struct list_head *a, struct list_head *b) {
	struct data* x = list_entry(a, struct data, list);
	struct data* y = list_entry(b, struct data, list);

	if (x->i > y->i) return 1;
	else if (x->i < y->i) return -1;
	else return 0;
}

static void __list_sort(struct list_head* head) {
	list_sort(NULL, head, cmp);
}

static void copy_list(struct list_head* dst, struct list_head* src) {
	struct data *d, *tmp, *n;

	list_for_each_entry(n, src, list) {
		d = malloc(sizeof(struct data));
		*d = *n;
		
		list_add_tail(&d->list, dst);
	}
}

static void free_second_half_list(struct list_head* head) {
	struct list_head* pos;
	struct data *d, *tmp;
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
	struct data *d, *tmp;
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

static void print_list(struct list_head* head, int id) {
	struct data *d;

	printf("thread[%d]: ", id);
	list_for_each_entry(d, head, list) {
		printf("%d ->", d->i);
	}
	printf("\n");
}
#if 1
static void pflush_master(void *arg) {
	int cpu = (int)arg;
	int id = cpu, i, n = NUM_THREAD;
	struct data *d;

	bind_to_cpu(cpu);

	__list_sort(&heads[id]);
}
#endif
#if 0
static void pflush_master(void *arg) {
	int cpu = (int)arg;
	int id = cpu, i, n = NUM_THREAD;
	struct data *d;

	bind_to_cpu(cpu);
	//printf("thread[%d] running on cpu[%d]\n", id, cpu);

	pthread_barrier_wait(&barrier);

	__list_sort(&heads[id]);

/*
	list_for_each_entry(d, &heads[id], list) {
		printf("thread[%d]: %d\n", id, d->i);
	}
*/
	pthread_barrier_wait(&barrier);

	for (i = 0; i < n; i ++) {
		//printf("thread[%d]-----------------phase [%d]---------------\n", id, i);
		if (i % 2 == 0) {
			if (id % 2 == 0) {
				list_splice(&heads[id], &heads[id + 1]);
				INIT_LIST_HEAD(&heads[id]);
				copy_list(&heads[id], &heads[id + 1]);
				pthread_barrier_wait(&barrier);
				
				__list_sort(&heads[id]);
				free_second_half_list(&heads[id]);
				pthread_barrier_wait(&barrier);	
			} else {
				pthread_barrier_wait(&barrier);
				__list_sort(&heads[id]);
				free_first_half_list(&heads[id]);
				pthread_barrier_wait(&barrier);	
			}
		} else {
			if (id % 2 == 0) {
				pthread_barrier_wait(&barrier);
				if (id != 0) {
					__list_sort(&heads[id]);
					free_first_half_list(&heads[id]);
				}
				pthread_barrier_wait(&barrier);	
			} else {
				if (id != NUM_THREAD - 1) {
					list_splice(&heads[id], &heads[id + 1]);
					INIT_LIST_HEAD(&heads[id]);
					copy_list(&heads[id], &heads[id + 1]);
				}

				pthread_barrier_wait(&barrier);
				if (id != NUM_THREAD - 1) {
					__list_sort(&heads[id]);
					free_second_half_list(&heads[id]);
				}
				pthread_barrier_wait(&barrier);	
			}
		}
	}

	printf("thread[%d] exit\n", id);
	
	print_list(&heads[id], id);
}
#endif
static void pflush_worker(void *arg) {
	int cpu = (int)arg;
	int id = cpu, i, n = NUM_THREAD;
	struct data *d;

	bind_to_cpu(cpu);
	printf("thread[%d] running on cpu[%d]\n", id, cpu);

	pthread_barrier_wait(&barrier);

	__list_sort(&heads[id]);
/*
	list_for_each_entry(d, &heads[id], list) {
		printf("thread[%d]: %d\n", id, d->i);
	}
*/
	pthread_barrier_wait(&barrier);

	for (i = 0; i < n; i ++) {
		//printf("thread[%d]-----------------phase [%d]---------------\n", id, i);
		if (i % 2 == 0) {
			if (id % 2 == 0) {
				list_splice(&heads[id], &heads[id + 1]);
				INIT_LIST_HEAD(&heads[id]);
				copy_list(&heads[id], &heads[id + 1]);
				pthread_barrier_wait(&barrier);
				
				__list_sort(&heads[id]);
				free_second_half_list(&heads[id]);
				pthread_barrier_wait(&barrier);	
			} else {
				pthread_barrier_wait(&barrier);
				__list_sort(&heads[id]);
				free_first_half_list(&heads[id]);
				pthread_barrier_wait(&barrier);	
			}
		} else {
			if (id % 2 == 0) {
				pthread_barrier_wait(&barrier);
				if (id != 0) {
					__list_sort(&heads[id]);
					free_first_half_list(&heads[id]);
				}
				pthread_barrier_wait(&barrier);	
			} else {
				if (id != NUM_THREAD - 1) {
					list_splice(&heads[id], &heads[id + 1]);
					INIT_LIST_HEAD(&heads[id]);
					copy_list(&heads[id], &heads[id + 1]);
				}

				pthread_barrier_wait(&barrier);
				if (id != NUM_THREAD - 1) {
					__list_sort(&heads[id]);
					free_second_half_list(&heads[id]);
				}
				pthread_barrier_wait(&barrier);	
			}
		}
	}

	printf("thread[%d] exit\n", id);
	//print_list(&heads[id], id);
}

static void thread_init() {
	int i;

	pthread_barrier_init(&barrier, NULL, NUM_THREAD);

	for (i = 0; i < NUM_THREAD; i++) {
/*
		if (pthread_create(&tids[i], NULL,
			(i == 0) ? (void*)pflush_master : (void*)pflush_worker, (void*)i) != 0) {
        		printf("bonsai create thread[%d] failed\n", i);
    	}
*/
		//if (pthread_create(&tids[i], NULL, (void*)pflush_master, (void*)i) != 0) {	
		if (pthread_create(&tids[i], NULL, (void*)pflush_worker, (void*)i) != 0) {
        		printf("bonsai create thread[%d] failed\n", i);
    	}
	}
}

int main() {
	int i;
	
	list_init();
	thread_init();

	for (i = 0; i < NUM_THREAD; i ++) {
		pthread_join(tids[i], NULL);
	}

	for (i = 0; i < NUM_THREAD; i ++) {
		print_list(&heads[i], i);
	}
/*
	struct list_head head1,head2;
	struct data a,b,c,d,*pos;
	INIT_LIST_HEAD(&head1);
	INIT_LIST_HEAD(&head2);

	a.i=2;b.i=1;
	list_add(&a.list, &head1);
	list_add(&b.list, &head1);

	c.i=4;d.i=3;
	list_add(&c.list, &head2);
	list_add(&d.list, &head2);

	list_splice(&head1, &head2);
	list_for_each_entry(pos, &head2, list) {
		printf("%d\n", pos->i);
	}
*/
	return 0;
}
