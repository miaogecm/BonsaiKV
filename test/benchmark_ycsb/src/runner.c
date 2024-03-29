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
#include "loader.h"
#include "runner.h"
#include "config.h"
#include "pcm.h"

static __thread struct timeval t0, t1;

struct task {
    int id;
    int nr_stage;
    const char *(**stage_func)(struct kvstore *, void *, int);
    struct kvstore *kv;
    void *context;
    pthread_barrier_t *barrier;
};

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

static inline void start_measure() {
    gettimeofday(&t0, NULL);
}

static inline double end_measure() {
    gettimeofday(&t1, NULL);
    return t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
}

static inline int cpu_of(int id) {
    return cpuseq[id % NUM_CPU];
}

static void* run_task(void* arg) {
    struct task *task = arg;
    const char *name;
    double interval;
    void *tcontext;
    int stage;

    bind_to_cpu(cpu_of(task->id));
    printf("worker %d bind to cpu %d\n", task->id, cpu_of(task->id));

    tcontext = task->kv->kv_thread_create_context(task->context, task->id);

    for (stage = 0; stage < task->nr_stage; stage++) {
        if (task->id == 0) {
            task->kv->kv_start_test(task->context);
#ifdef USE_PCM
            pcm_start();
#endif
        }

        pthread_barrier_wait(task->barrier);

        task->kv->kv_thread_start_test(tcontext);

        start_measure();

        name = task->stage_func[stage](task->kv, tcontext, task->id);

        interval = end_measure();
        printf("%s[%d] finished in %.3lf seconds\n", name, task->id, interval);

        task->kv->kv_thread_stop_test(tcontext);

        pthread_barrier_wait(task->barrier);

        if (task->id == 0) {
            interval = end_measure();
            printf("%s: foreground done, %.3lf seconds elapsed\n", name, interval);

#ifdef USE_PCM
            printf("%s: remote access: %lu packets\n", name, pcm_get_nr_remote_pmem_access_packet());
#endif

            task->kv->kv_stop_test(task->context);

            interval = end_measure();
            printf("%s: background done, %.3lf seconds elapsed\n", name, interval);
        }
    }

    pthread_barrier_wait(task->barrier);

    task->kv->kv_thread_destroy_context(tcontext);

    return NULL;
}

static inline void configure(struct kvstore *kv, void *conf) {
    const char *engine = kv->kv_engine();
    if (!strcmp(engine, "bonsai")) {
        struct bonsai_config *c = conf;
        int id;
        c->nr_user_cpus = NUM_THREADS;
        c->user_cpus = malloc(sizeof(*c->user_cpus) * NUM_THREADS);
        for (id = 0; id < NUM_THREADS; id++) {
            c->user_cpus[id] = cpu_of(id);
        }
    } else if (!strcmp(engine, "pacman")) {
        struct pacman_config *c = conf;
        c->num_workers = NUM_THREADS;
    } else if (!strcmp(engine, "lbtree")) {
        struct lbtree_config *c = conf;
        c->nr_workers = NUM_THREADS;
    }
}

int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) __attribute__((weak));

static void check_malloc() {
    if (mallctl) {
        printf("Using 3rd-party malloc.\n");
    } else {
        printf("Using glibc malloc.\n");
    }
}

void run_kvstore(struct kvstore *kv, void *conf, int nr_stage,
                 const char *(*stage_func[])(struct kvstore *kv, void *tcontext, int)) {
    struct task tasks[NUM_THREADS];
    pthread_t tids[NUM_THREADS];
    pthread_barrier_t barrier;
    void *context;
    int i;

    check_malloc();

    configure(kv, conf);

    pthread_barrier_init(&barrier, NULL, NUM_THREADS);

    context = kv->kv_create_context(conf);

    for (i = 0; i < NUM_THREADS; i++) {
        tasks[i].kv = kv;
        tasks[i].context = context;
        tasks[i].nr_stage = nr_stage;
        tasks[i].stage_func = stage_func;
        tasks[i].barrier = &barrier;
        tasks[i].id = i;
    }

    for (i = 0; i < NUM_THREADS; i++) {
        pthread_create(&tids[i], NULL, run_task, &tasks[i]);
        pthread_setname_np(tids[i], "run_task");
    }

    for (i = 0; i < NUM_THREADS; i++) {
        pthread_join(tids[i], NULL);
    }

    kv->kv_destroy_context(context);
}
