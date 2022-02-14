#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <sched.h>
#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/time.h>

#include "bptree.h"

#include "../data/kvdata.h"

#ifndef N
#define N			1000000
#endif

uint64_t a[N];

#ifndef NUM_THREAD
#define NUM_THREAD	4
#endif

#define NUM_CPU		8

b_root_obj *bp;
pthread_t tids[NUM_THREAD];

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1)
		perror("sched_getaffinity fail\n");

	for (i = 0; i < NUM_CPU; i++) {
		if (CPU_ISSET(i, &mask))
			return i;
	}

	return -1;
}

static inline void bind_to_cpu(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind cpu failed\n");
    }
}

static inline void die() {
	assert(0);
	exit(1);
}

static pthread_barrier_t barrier;

static __thread struct timeval t0, t1;

static inline void start_measure() {
    gettimeofday(&t0, NULL);
}

static inline double end_measure() {
    gettimeofday(&t1, NULL);
    return t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
}

static void do_load(long id) {
    double interval;
	long i;
    if (id == 0) {
        start_measure();
        for (i = 0; i < N; i ++) {
            assert(bp_insert(bp, load_arr[i][0], load_arr[i][1]) == 0);
        }
        interval = end_measure();
        printf("load finished in %.3lf seconds\n", interval);

        interval = end_measure();
        printf("load total: %.3lf seconds\n", interval);
    }
}

static void do_op(long id) {
	uint64_t v = 0;
	uint64_t* val_arr = malloc(sizeof(uint64_t*) * N);
    double interval;
	long i, repeat = 1;

    start_measure();

    while(repeat--) {
        for (i = 0; i < N; i ++) {

            switch (op_arr[id][i][0]) {
            case 0:
                bp_insert(bp, op_arr[id][i][1], op_arr[id][i][2]);
                break;
            case 1:
                bp_insert(bp, op_arr[id][i][1], op_arr[id][i][2]);
                break;
            case 2:
                bp_lookup(bp, op_arr[id][i][1]);
                break;
            case 3:
                bp_scan(bp, op_arr[id][i][1], op_arr[id][i][2], val_arr);
                break;
            default:
                printf("unknown type\n");
                assert(0);
                break;
            }
        }
    }

    interval = end_measure();
    printf("user thread[%ld]: workload finished in %.3lf seconds\n", id, interval);

	printf("user thread[%ld]---------------------end---------------------\n", id);

	free(val_arr);

	printf("user thread[%ld] exit\n", id);
}

void* thread_fun(void* arg) {
	long id = (long)arg;

	bind_to_cpu(id);

    pthread_barrier_wait(&barrier);

    do_load(id);

    pthread_barrier_wait(&barrier);

    do_op(id);

    pthread_barrier_wait(&barrier);

    if (id == 0) {
        double interval = end_measure();
        printf("op total: %.3lf seconds\n", interval);
    }

	return NULL;
}

int bench() {
    long i;

	bind_to_cpu(0);

    bp = bp_init();

    pthread_barrier_init(&barrier, NULL, NUM_THREAD);

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
        pthread_setname_np(tids[i], "user_thread");
	}
	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}

out:
	return 0;
}

int main() {
    bench();
    return 0;
}