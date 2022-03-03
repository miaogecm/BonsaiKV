/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <signal.h>
#include <stdio.h>
#include <sys/time.h>

#include "thread.h"
#include "region.h"
#include "oplog.h"
#include "common.h"
#include "bonsai.h"
#include "arch.h"
#include "epoch.h"
#include "cpu.h"

extern struct bonsai_info* bonsai;

typedef void (*signal_handler_t)(int);

void try_run_epoch() {
	int cpu = __this->t_cpu;
	struct log_layer* layer = LOG(bonsai);
	struct log_region *region = &layer->region[cpu];
	__le64 old_epoch = bonsai->desc->epoch, new_epoch = old_epoch + 1;

    if (likely(atomic_read(&layer->epoch_passed) <= 1 || __this->t_epoch == new_epoch)) {
        return;
    }

    bonsai_print("user thread[%d]: run epoch %llu\n", __this->t_id, old_epoch);

	/* persist it */
    if (likely(region->curr_blk)) {
        bonsai_flush((void *) region->curr_blk, sizeof(struct oplog_blk), 1);
    }
    __this->t_epoch = new_epoch;

    /* If I'm the last one, update the global version, and allow next epoch. */
    if (atomic_add_return(-1, &layer->epoch_passed) == 1) {
        bonsai->desc->epoch = new_epoch;
        bonsai_flush(&bonsai->desc->epoch, sizeof(__le64), 0);

        /* @atomic_dec provides an implicit full fence. */
        atomic_dec(&layer->epoch_passed);

        bonsai_print("user thread[%d]: update global epoch to %llu\n", __this->t_id, new_epoch);
    }
}

void thread_alarm_handler(int sig) {
	struct log_layer* layer = LOG(bonsai);
    atomic_cmpxchg(&layer->epoch_passed, 0, NUM_USER_THREAD + 1);
}

/*
 * thread_block_alarm_signal: bonsai pflush thread block this signal
 */
int thread_block_alarm_signal() {
	sigset_t set;
	int ret = 0;

	sigemptyset(&set);
	sigaddset(&set, SIGALRM);

	ret = pthread_sigmask(SIG_BLOCK, &set, NULL);
	if (ret)
		perror("block alarm signal\n");

	return ret;
}

/*
 * thread_register_alarm_signal: the master thread must register this signal
 */
int thread_register_alarm_signal() {
	struct itimerval value;
	struct sigaction sa;
	int ret = 0;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = thread_alarm_handler;
	if (sigaction(SIGALRM, &sa, NULL) == -1) {
		perror("sigaction\n");
        goto out;
	}

	value.it_interval.tv_sec = 100000;
	value.it_interval.tv_usec = EPOCH;
    value.it_value = value.it_interval;

    ret = setitimer(ITIMER_REAL, &value, NULL);
	if (ret) {
		perror("setitimer\n");
        goto out;
	}

out:
	return ret;
}
