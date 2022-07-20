/*
 * A driver for TPC-C and TATP Benchmark
 */

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

#include "rand.h"
#include "workload.h"
#include "bench.h"

struct bench bench;
int num_w;

static pthread_barrier_t barrier;
static __thread struct timeval t0, t1;
pthread_t tids[MAX_THREADS];

static int num_threads;
static int num_works;
static int num_cpu;

static int run_tpcc = 1;

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1)
		perror("sched_getaffinity fail\n");

	for (i = 0; i < num_cpu; i++) {
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

static inline void start_measure() {
    gettimeofday(&t0, NULL);
}

static inline double end_measure() {
    gettimeofday(&t1, NULL);
    return t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
}

static void* tpcc_load_thread_fun(void* arg) {
    int id = (long)arg;
    double interval;
    int st, ed;
    int i, j;
    
    bind_to_cpu(id % num_cpu);

    pthread_barrier_wait(&barrier);

    start_measure();

    if (id == 0) {
        tpcc_load_item();
        tpcc_load_warehouse();
    }
    
    st = 1.0 * id / num_threads * (num_w + 1);
    ed = 1.0 * (id + 1) / num_threads * (num_w + 1);
    for (i = st; i < ed; i++) {
        tpcc_load_stock(i);
        tpcc_load_district(i);
        for (j = 1; j <= NUM_D; j++) {
            tpcc_load_customer(id, i, j);
            tpcc_load_order(i, j);
        }
    }

    interval = end_measure();
    printf("load[%d] finished in %.3lf seconds\n", id, interval);

    pthread_barrier_wait(&barrier);

    if (id == 0) {
        interval = end_measure();
        printf("LOAD: %.3lf seconds elapsed\n", interval);
    }
    return NULL;
}

static void* tatp_load_thread_fun(void *arg) {
    int id = (long) arg;
    double interval;

    bind_to_cpu(id % num_cpu);
    pthread_barrier_wait(&barrier);
    start_measure();

    if (id <= num_threads / 10) { 
        int numThreadsInsertingSubscribers = num_threads / 10 + 1;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int start = id * numSubscribersPerThread;
        int end = (id + 1) * numSubscribersPerThread;
        tatp_loadSubscriber(start, end);
    } else if (num_threads / 10 < id && id <= num_threads / 10 + num_threads / 4) {
        int numThreadsInsertingSubscribers = num_threads / 4;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int newId = id - num_threads / 10 - 1;
        int start = newId * numSubscribersPerThread;
        int end = (newId + 1) * numSubscribersPerThread;
        tatp_loadAccessInfo(start, end);
    } else {
        int numThreadsInsertingSubscribers = num_threads - num_threads / 4 - num_threads / 10 - 1;
        int numSubscribersPerThread = NUM_SUBSCRIBER / numThreadsInsertingSubscribers;
        int newId = id - (num_threads / 10 + num_threads / 4) - 1;
        int start = newId * numSubscribersPerThread;
        int end = (newId + 1) * numSubscribersPerThread;
        tatp_loadSpecialFacility_CallForwarding(start, end);
    }

    interval = end_measure();
    printf("load[%d] finished in %.3lf seconds\n", id, interval);

    pthread_barrier_wait(&barrier);

    if (id == 0) {
        interval = end_measure();
        printf("LOAD: %.3lf seconds elapsed\n", interval);
    }
    return NULL;
}

static void* load_thread_parent_fun(void *arg) {
	long i;
	bind_to_cpu(0);

	for (i = 0; i < num_threads; i++) {
		pthread_create(&tids[i], NULL, 
			run_tpcc ? tpcc_load_thread_fun : tatp_load_thread_fun, (void*)i);
        pthread_setname_np(tids[i], "load_thread");
	}
	for (i = 0; i < num_threads; i++) {
		pthread_join(tids[i], NULL);
	}
    return NULL;
}

static void* bench_thread_fun(void* arg) {
    int id = (long)arg;
    double interval;
    int num_works_left;

    bind_to_cpu(id % num_cpu);

    num_works_left = num_works / num_threads; 
    if (id == 0) {
        num_works_left += num_works % num_threads;
    }

    pthread_barrier_wait(&barrier);

    start_measure();

    while(num_works_left--) {
        if (run_tpcc) tpcc_work(id, get_w_id());
		else tatp_work(id);
    }

    interval = end_measure();
    printf("work[%d] finished in %.3lf seconds\n", id, interval);

    pthread_barrier_wait(&barrier);

    if (id == 0) {
        interval = end_measure();
        printf("WORK: %.3lf seconds elapsed\n", interval);
    }
    return NULL;
}

static void* bench_thread_parent_fun(void *arg) {
    long i;
	bind_to_cpu(0);
    for (i = 0; i < num_threads; i++) {
		pthread_create(&tids[i], NULL, bench_thread_fun, (void*)i);
        pthread_setname_np(tids[i], "bench_thread");
	}
	for (i = 0; i < num_threads; i++) {
		pthread_join(tids[i], NULL);
	}
    return NULL;
}

extern void bench_init(init_func_t init, destroy_func_t destroy,
				      insert_func_t insert, update_func_t update, remove_func_t remove,
				      lookup_func_t lookup, scan_func_t scan) {
    int i;

    bench.init = init;
    bench.destroy = destroy;
    bench.insert = insert;
    bench.update = update;
    bench.remove = remove;
    bench.lookup = lookup;
    bench.scan = scan;

    for (i = 0; i < MAX_THREADS; i++) {
        atomic_set(&bench.h_pk[i], 0);
    }

    bench.kv = bench.init();
}

extern void bench_destroy() {
    bench.destroy(bench.kv);
}

extern void bench_set_env(int __num_w, int __num_works, int __num_cpu) {
    assert(__num_w > 1);
    num_w = __num_w;
    num_works = __num_works;
    num_cpu = __num_cpu;
}

extern void bench_load(int __num_threads) {
    pthread_t load_thread_parent;

    num_threads = __num_threads;
    bind_to_cpu(0);

    pthread_barrier_init(&barrier, NULL, num_threads);
    pthread_create(&load_thread_parent, NULL, load_thread_parent_fun, NULL);
    pthread_setname_np(load_thread_parent, "load_thread_parent");
    pthread_join(load_thread_parent, NULL);
}

extern void run_bench(int __num_threads) {
    pthread_t bench_thread_parent;

    num_threads = __num_threads;
    bind_to_cpu(0);

    pthread_barrier_init(&barrier, NULL, num_threads);
    pthread_create(&bench_thread_parent, NULL, bench_thread_parent_fun, NULL);
    pthread_setname_np(bench_thread_parent, "bench_thread_parent");
    pthread_join(bench_thread_parent, NULL);
}
