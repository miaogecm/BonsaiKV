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

static void main_alarm_handler(int sig) {
	struct log_layer* layer = LOG(bonsai);
	layer->epoch ++;

	bonsai_debug("bonsai epoch[%d]\n", layer->epoch);
}

void thread_alarm_handler(int sig) {
	struct log_layer* layer = LOG(bonsai);
	struct log_region *region = &layer->region[get_cpu()];
	
	/* persist it */
	region->curr_blk->flush = cpu_to_le8(1);
	bonsai_flush(region->curr_blk, sizeof(struct oplog_blk), 1);
}

static int register_alarm(signal_handler_t handler) {
	struct itimerval value;
	struct sigaction sa;
	int err = 0;

	memset(&value, 0, sizeof(struct itimerval));
	value.it_interval.tv_sec = 0;
	value.it_interval.tv_usec = EPOCH;

	err = setitimer(ITIMER_VIRTUAL, &value, NULL);
	if (err)
		return err;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = handler;
	if (sigaction(SIGALRM, &sa, NULL) == -1) {
		err = -ESIGNO;
	}

	return err;
}

void thread_block_alarm() {
	sigset_t set;

	sigemptyset(&set);
	sigaddset(&set, SIGALRM);

	pthread_sigmask(SIG_BLOCK, &set, NULL);
}

int thread_set_alarm() {
	return register_alarm(thread_alarm_handler);
}

int epoch_init() {
	return register_alarm(main_alarm_handler);
}
