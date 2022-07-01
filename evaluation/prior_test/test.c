/*
 * Intel PMEM Performance Test
 */

#define _GNU_SOURCE

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint-gcc.h>
#include <malloc.h>

#include "arch.h"
#include "measure.h"
#include "pool.h"

#define POOL_SIZE           25769803776ul
//#define POOL_SIZE           4294967296ul
#define NUM_CPU             2
#define LOCAL_POOL_SIZE     (POOL_SIZE / NUM_CPU)

struct task_struct {
    int id;
    void *addr;
    pthread_t thread;
    sem_t *mutex;
};

static struct task_struct workers[NUM_CPU];

static sem_t mutex[6];

#define N 64
static __thread int arr[N];

int cmp(const void *p, const void *q) {
    int a = *(int *) p, b = *(int *) q;
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
}

void do_something() {
    int i;
    for (i = 0; i < N; i++) {
        arr[i] = N - i;
    }
    qsort(arr, N, sizeof(int), cmp);
}

#include <libpmem.h>

#define WRITE_NT_64_ASM \
    "vmovntdq %%zmm0, 0(%[addr]) \n"
                        \
void *run_worker(void *task_) {
    struct task_struct *task = task_;
    int i, rep = 100, stride = 64, j;
    double time;
    void *p, *q;
    size_t off;
    unsigned long num = 0;

    const char memdata[64] __attribute__((aligned(64))) = "TT14EZSYIROGYY2MWI6DH7TO3FZ2O7TO03PSVPKWEJK1KNU6XOLR803FE9CET8ZO";

    bind_to_cpu(2 * task->id);
    printf("Worker %d start to run, from %lx to %lx %lx\n", task->id, task->addr, LOCAL_POOL_SIZE, task->mutex);

    //sem_wait(task->mutex);

    if (task->id == 0) {
        stride = 64;
    } else {
        stride = 4;
    }

    while (rep--) {
        start_measure();
        num = 0;

        for (off = 0; off < LOCAL_POOL_SIZE; off += stride * CACHELINE_SIZE) {
            p = task->addr + off;

            for (i = 0; i < stride; i++) {
                asm volatile(
                "vmovntdqa  (%[data]), %%zmm0 \n"
                WRITE_NT_64_ASM
                :
                : [addr] "r"(p + i * CACHELINE_SIZE), [data] "r"(memdata)
                : "%zmm0"
                );
            }
            sfence();

            // block

            //do_something();
            //sfence();

            //mfence();
            //sfence();
            //smp_mb();
            num++;
        }

        time = end_measure();
        printf("================= Worker %d report, time: %lfs, throughput: %lf\n", task->id, time, num * stride * CACHELINE_SIZE / time / 1073741824.0);
    }
    printf("%d\n", num);

    sfence();

    time = end_measure();

    printf("================= Worker %d end, time: %lfs, throughput: %lf\n", task->id, time, num * stride * CACHELINE_SIZE / time / 1073741824.0);

    //sem_post(task->mutex);
}

#define NUM_DIMMS   1

void run_test() {
    void *dimms[] = {
            nvm_create_pool(0, POOL_SIZE)
    }, *addr;

    int i, cpu, dimm;
    for (dimm = 0; dimm < NUM_DIMMS; dimm++) {
        sem_init(&mutex[dimm], 0, 1);
    }
    for (dimm = 0, cpu = 0; dimm < NUM_DIMMS; dimm++) {
        for (i = 0; i < NUM_CPU; i++, cpu++) {
            addr = dimms[dimm] + LOCAL_POOL_SIZE * i;
            workers[cpu].mutex = &mutex[dimm];
            workers[cpu].id = cpu;
            workers[cpu].addr = addr;
            pthread_create(&workers[cpu].thread, NULL, run_worker, &workers[cpu]);
        }
    }
    for (dimm = 0, cpu = 0; dimm < NUM_DIMMS; dimm++) {
        for (i = 0; i < NUM_CPU; i++, cpu++) {
            pthread_join(workers[cpu].thread, NULL);
        }
    }
}
