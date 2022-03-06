#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <assert.h>

#define MAX     10000000
#define MIN	    0

#define NUM_THREAD      6
#define PER_THREAD      1000000

static int *data[NUM_THREAD];

pthread_t tids[NUM_THREAD];

pthread_barrier_t barrier;

static void bind_to_cpu(int cpu) {
    int ret;
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((ret = sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        printf("glibc init: bind cpu[%d] failed.\n", cpu);
    }
}

#define sort_cmp(x, y, arg)    ((*(int *) (x)) == (*(int *) (y)) ? 0 : ((*(int *) (x)) < (*(int *) (y)) ? -1 : 1))
#include "sort.h"

static inline int __random() {
	return rand() % MAX + MIN;
}

static void load_data() {
    int i, j;

	srand((unsigned)time(NULL));

    for (i = 0; i < NUM_THREAD; i++) {
        data[i] = malloc(sizeof(int) * PER_THREAD);
        for (j = 0; j < PER_THREAD; j++) {
            data[i][j] = __random();
        }
    }
}

static void quick_sort(int *arr) {
    __qsort(arr, PER_THREAD, sizeof(int), NULL);
}

#define unlikely(x) __builtin_expect((unsigned long)(x), 0)

static void pflush_worker_fast(void *arg) {
	int cpu = (int)arg;
	int id = cpu, phase, n = NUM_THREAD, i, left, oid;
    int *my, *other, **get, *new_my, *new_ptr, delta;

	bind_to_cpu(cpu);
	printf("thread[%d] running on cpu[%d]\n", id, cpu);

    quick_sort(data[id]);

	for (phase = 0; phase < n; phase ++) {
        /* Wait for the last phase to be done. */
	    pthread_barrier_wait(&barrier);

		printf("thread[%d]-----------------phase [%d]---------------\n", id, phase);

        /* Am I the left half in this phase? */
        left = id % 2 == phase % 2;
        delta = left ? 1 : -1;
        oid = id + delta;
        if (oid < 0 || oid >= NUM_THREAD) {
            pthread_barrier_wait(&barrier);
            continue;
        }

        my = left ? data[id] : (data[id] + PER_THREAD - 1);
        other = left ? data[oid] : (data[oid] + PER_THREAD - 1);

        new_my = malloc(sizeof(int) * PER_THREAD);
        new_ptr = left ? new_my : (new_my + PER_THREAD - 1);

        for (i = 0; i < PER_THREAD; i++) {
            if (unlikely(my - data[id] == PER_THREAD || my < data[id])) {
                get = &other;
            } else if (unlikely(other - data[oid] == PER_THREAD || other < data[oid])) {
                get = &my;
            } else {
                if (*my == *other) {
                    get = &my;
                } else {
                    get = (!left ^ (*my < *other)) ? &my : &other;
                }
            }
            *new_ptr = **get;
            *get += delta;
            new_ptr += delta;
        }

	    pthread_barrier_wait(&barrier);

        free(data[id]);
        data[id] = new_my;
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

static void check() {
	int *pos, *ppos = NULL;
    int i, j;

    for (i = 0; i < NUM_THREAD; i++) {
        for (j = 0; j < PER_THREAD; j++) {
            pos = &data[i][j];
            if (ppos && *pos < *ppos) {
                printf("WRONG!!!\n");
                exit(1);
            }
            ppos = pos;
        }
    }
}

int main() {
    int i;

    load_data();

	thread_init();

	for (i = 0; i < NUM_THREAD; i ++) {
		pthread_join(tids[i], NULL);
	}

    check();
}