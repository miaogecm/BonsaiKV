/*
 * Intel PMEM Performance Test
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#include "arch.h"
#include "pool.h"
#include "measure.h"
#include "at.h"

struct task_struct {
    struct at_policy *policy;
    int id;
    va_t base;
    pthread_t thread;
    size_t region_size;

    double time;
};

static struct task_struct workers[NUM_CPU_PER_SOCKET];

static void *dimms[CONFIG_DIMM_CNT];

void *run_worker(void *task_) {
    struct task_struct *task = task_;
    int rep = 1, cpu, dimm, i;
    size_t stride, sum = 0;
    void *pa;

    cpu = node_to_cpu(CONFIG_NUMA_NODE, task->id);
    bind_to_cpu(cpu);

    printf("Worker %d start to run at CPU %d, from %lx, size %lu\n", task->id, cpu, task->base, task->region_size);

    start_measure();

    while (rep--) {
        for (stride = 0; stride < task->region_size / CONFIG_DIMM_CNT; stride += CONFIG_CHUNK_SIZE) {
            for (dimm = 0; dimm < CONFIG_DIMM_CNT; dimm++) {
                pa = task->policy->pa_bases[dimm] + task->base / CONFIG_DIMM_CNT + stride;
                for (i = 0; i < CONFIG_CHUNK_SIZE / CACHELINE_SIZE; i++, pa += CACHELINE_SIZE) {
                    sum += *(unsigned long *) pa;
                }
            }
        }
    }

    task->time = end_measure();

    printf("================= Worker %d end, time: %lfs, sum: %lu\n", task->id, task->time, sum);
}

double run_test(int num_worker) {
    size_t size_per_dimm = CONFIG_SIZE / CONFIG_DIMM_CNT;
    size_t region_size = CONFIG_SIZE / num_worker;
    struct at_policy policy;
    double max_time = 0.0;
    int i;

    printf("###!!! CHUNK SIZE: %lu, NUM WORKER: %d !!!###\n", CONFIG_CHUNK_SIZE, num_worker);

    for (i = 0; i < CONFIG_DIMM_CNT; i++) {
        dimms[i] = nvm_create_pool(node_dimm_id(CONFIG_NUMA_NODE, i), size_per_dimm);
        policy.pa_bases[i] = dimms[i];
    }

    for (i = 0; i < num_worker; i++) {
        workers[i].region_size = region_size;
        workers[i].policy = &policy;
        workers[i].id = i;
        workers[i].base = region_size * i;
        pthread_create(&workers[i].thread, NULL, run_worker, &workers[i]);
    }

    for (i = 0; i < num_worker; i++) {
        pthread_join(workers[i].thread, NULL);
    }

    for (i = 0; i < num_worker; i++) {
        if (workers[i].time > max_time) {
            max_time = workers[i].time;
        }
    }

    for (i = 0; i < CONFIG_DIMM_CNT; i++) {
        nvm_destroy_pool(dimms[i], size_per_dimm);
    }

    return max_time;
}
