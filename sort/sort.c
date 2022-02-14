#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include "list.h"

#define MAX     100000	
#define MIN	0

static int N[4] = {3021,2959,3026,2875};

#define NUM_THREAD		4
struct list_head heads[NUM_THREAD];
pthread_t tids[NUM_THREAD];

pthread_barrier_t barrier;

static int x = 0;

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
        bonsai_print("glibc init: bind cpu[%d] failed.\n", cpu);
    }
}

static int print_list(struct list_head* head, int id) {
	struct data *d;
	int sum = 0;

	bonsai_print("thread[%d]: ", id);
	list_for_each_entry(d, head, list) {
		bonsai_print("%d ->", d->i);
		sum++;
	}
	bonsai_print("\n");

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
			//data->i = __random();
			data->i = x; x++;
			list_add(&data->list, &heads[i]);
		}
		bonsai_print("list [%d] init\n", i);
	}

}

static int cmp(void *priv, struct list_head *a, struct list_head *b) {
	struct data* x = list_entry(a, struct data, list);
	struct data* y = list_entry(b, struct data, list);

	if (x->i > y->i) return 1;
	else if (x->i < y->i) return -1;
	else return 0;
}

static void check(struct list_head* head) {
	int i=0,j, a[10000], sum = 0;
	struct data* pos;

	if (list_empty(head))
		return;

	list_for_each_entry(pos, head, list) {
		a[i] = pos->i;
		sum ++; i ++;
	}

	for (i=0;i<sum;i++){
		for(j=0;j<sum;j++) {
			if (i!=j){
				if (a[i] == a[j]) {
					bonsai_print("%d a[%d]:%d a[%d]:%d a[%d]:%d a[%d]:%d\n", gettid(), i-1,a[i-1],i,a[i],j,a[j],j+1,a[j+1]);
					//print_list(head, 1);					
					exit(0);
				}
			}
		}
	}
}

static void __list_sort(struct list_head* head) {
	list_sort(NULL, head, cmp);
	bonsai_print("[%d]----------__list_sort------------\n",gettid());
	check(head);
}

static void copy_list(struct list_head* dst, struct list_head* src) {
	struct data *d, *tmp, *n;

	list_for_each_entry(n, src, list) {
		d = malloc(sizeof(struct data));
		*d = *n;
		
		list_add_tail(&d->list, dst);
	}
	bonsai_print("[%d]----------copy_list------------\n",gettid());
	check(dst);
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
	bonsai_print("thread[%d] running on cpu[%d]\n", id, cpu);

	pthread_barrier_wait(&barrier);

	__list_sort(&heads[id]);

	pthread_barrier_wait(&barrier);

	for (i = 0; i < n; i ++) {
		bonsai_print("thread[%d]-----------------phase [%d]---------------\n", id, i);
		if (i % 2 == 0) {
			if (id % 2 == 0) {		
				list_splice(&heads[id], &heads[id + 1]);
				bonsai_print("[%d]----------list_splice------------\n",gettid());
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

	bonsai_print("thread[%d] exit\n", id);
	//print_list(&heads[id], id);
}

static void thread_init() {
	int i;
	pthread_barrier_init(&barrier, NULL, NUM_THREAD);

	for (i = 0; i < NUM_THREAD; i++) {
		if (pthread_create(&tids[i], NULL, (void*)pflush_worker, (void*)i) != 0) {
        		bonsai_print("bonsai create thread[%d] failed\n", i);
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

	for (i = 0; i < NUM_THREAD; i ++) {
		sum += print_list(&heads[i], i);
	}

	bonsai_print("sum: %d\n", sum);
	return 0;
}
