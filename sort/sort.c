#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <assert.h>
#include "list.h"

#define MAX     10000000
#define MIN	0

#define NUM_THREAD		6

static int N[NUM_THREAD] = {1200000,1190000,1220000,1210000,1200000,1190000};

struct list_head heads[NUM_THREAD];
pthread_t tids[NUM_THREAD];

pthread_barrier_t barrier;

static int x = 10000000;

struct data {
	int i;
	struct list_head list;
};

#define gettid() ((pid_t)syscall(SYS_gettid))

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

static int print_list(struct list_head* head, int id) {
	struct data *d;
	int sum = 0;

	printf("thread[%d]: ", id);
	list_for_each_entry(d, head, list) {
		printf("%d ->", d->i);
		sum++;
	}
	printf("\n");

	return sum;
}

static void list_init() {
	struct data *data;
	int i, j;

	srand((unsigned)time(NULL));

	for (i = 0; i < NUM_THREAD; i ++) {
		INIT_LIST_HEAD(&heads[i]);
		for (j = 0; j < N[i]; j ++) {
			data = malloc(sizeof(struct data));
			data->i = __random();
			//data->i = x; x--;
            //data->i = 1;
			list_add(&data->list, &heads[i]);
		}
		printf("list [%d] init\n", i);
	}

}

static inline int cmp(void *priv, struct list_head *a, struct list_head *b) {
	struct data* x = list_entry(a, struct data, list);
	struct data* y = list_entry(b, struct data, list);

	if (x->i > y->i) return 1;
	else if (x->i < y->i) return -1;
	else return 0;
}

static void check() {
	struct data* pos, *ppos = NULL;
    int i;

    for (i = 0; i < NUM_THREAD; i++) {
        list_for_each_entry(pos, &heads[i], list) {
            if (ppos && pos->i < ppos->i) {
                printf("WRONG!!!\n");
                exit(1);
            }
            ppos = pos;
        }
    }
}

static void __list_sort(struct list_head* head) {
	list_sort(NULL, head, cmp);
	printf("[%d]----------__list_sort------------\n",gettid());
}

static void copy_list(struct list_head* dst, struct list_head* src) {
	struct data *d, *tmp, *n;

	list_for_each_entry(n, src, list) {
		d = malloc(sizeof(struct data));
		*d = *n;
		
		list_add_tail(&d->list, dst);
	}
	printf("[%d]----------copy_list------------\n",gettid());
}

static void free_second_half_list(struct list_head* head) {
	struct list_head* pos;
	struct data *d, *tmp;
	int sum = 0, half, i = 0;

	list_for_each(pos, head) {
		sum ++; 
	}

	half = sum / 2;
	if (sum % 2) half += 1;

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

static void pflush_worker(void *arg) {
	int cpu = (int)arg;
	int id = cpu, i, n = NUM_THREAD;
	struct data *d;

	bind_to_cpu(cpu);
	printf("thread[%d] running on cpu[%d]\n", id, cpu);

	pthread_barrier_wait(&barrier);

	__list_sort(&heads[id]);

	pthread_barrier_wait(&barrier);

	for (i = 0; i < n; i ++) {
		printf("thread[%d]-----------------phase [%d]---------------\n", id, i);
		if (i % 2 == 0) {
			if (id % 2 == 0) {		
				list_splice(&heads[id], &heads[id + 1]);
				printf("[%d]----------list_splice------------\n",gettid());
				check(&heads[id + 1]);
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

#define unlikely(x) __builtin_expect((unsigned long)(x), 0)

static void pflush_worker_fast(void *arg) {
	int cpu = (int)arg;
	int id = cpu, phase, n = NUM_THREAD, i, left, oid;
    struct list_head *my, *other, *p1, *p2, **get, *next;
    struct data *e1, *e2;

	bind_to_cpu(cpu);
	printf("thread[%d] running on cpu[%d]\n", id, cpu);

	__list_sort(&heads[id]);

	for (phase = 0; phase < n; phase ++) {
        /* Wait for the last phase to be done. */
	    pthread_barrier_wait(&barrier);

		printf("thread[%d]-----------------phase [%d]---------------\n", id, phase);

        /* Am I the left half in this phase? */
        left = id % 2 == phase % 2;
        oid = id + (left ? 1 : -1);
        if (oid < 0 || oid >= NUM_THREAD) {
            pthread_barrier_wait(&barrier);
            continue;
        }

        my = &heads[id];
        other = &heads[oid];

        p1 = left ? my->next : my->prev;
        p2 = left ? other->next : other->prev;

        /*
         * Wait for each thread to have correct my->next/prev
         * and other->next/prev, which will be clobbered by
         * the following INIT_LIST_HEAD(my).
         */
        pthread_barrier_wait(&barrier);

        INIT_LIST_HEAD(my);
        for (i = 0; i < N[id]; i++) {
            if (unlikely(p1 == my)) {
                get = &p2;
            } else if (unlikely(p2 == other)) {
                get = &p1;
            } else {
                e1 = list_entry(p1, struct data, list);
                e2 = list_entry(p2, struct data, list);
                if (e1->i == e2->i) {
                    get = &p1;
                } else {
                    get = (!left ^ (e1->i < e2->i)) ? &p1 : &p2;
                }
            }
            assert(*get != my && *get != other);
            next = left ? (*get)->next : (*get)->prev;
            (left ? list_add_tail : list_add)(*get, my);
            *get = next;
        }
	}

	printf("thread[%d] exit\n", id);
	//print_list(&heads[id], id);
}

static void thread_init() {
	int i;
	pthread_barrier_init(&barrier, NULL, NUM_THREAD);

	for (i = 0; i < NUM_THREAD; i++) {
		if (pthread_create(&tids[i], NULL, (void*) pflush_worker_fast, (void*)i) != 0) {
        		printf("bonsai create thread[%d] failed\n", i);
    	}
	}
}

int main() {
	int i, sum = 0;
	
	list_init();
	thread_init();

	for (i = 0; i < NUM_THREAD; i ++) {
		pthread_join(tids[i], NULL);
	}

    //check();

#if 0
	for (i = 0; i < NUM_THREAD; i ++) {
		sum += print_list(&heads[i], i);
	}

	printf("sum: %d\n", sum);
#endif
	return 0;
}
