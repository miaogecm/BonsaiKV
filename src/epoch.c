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

void thread_alarm_handler(int sig) {
	int cpu = get_cpu();
	struct log_layer* layer = LOG(bonsai);
	struct log_region *region = &layer->region[cpu];
	__le64 old_epoch = ACCESS_ONCE(bonsai->desc->epoch);
	__le64 new_epoch = old_epoch + 1;

	if (ACCESS_ONCE(__this->t_epoch) == old_epoch) {
		/* At least a thread succeed */
		cmpxchg(&bonsai->desc->epoch, old_epoch, new_epoch);
	}

	/* persist it */
	bonsai_flush((void*)region->curr_blk, sizeof(struct oplog_blk), 1);

	/* re-allocate a new log block */
	region->curr_blk = alloc_oplog_block(cpu);

	__this->t_epoch ++;
}

static int register_alarm(signal_handler_t handler) {
	struct itimerval value;
	struct sigaction sa;
	int err = 0;

	memset(&value, 0, sizeof(struct itimerval));
	value.it_interval.tv_sec = 10000;
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

/*
 * thread_block_alarm: bonsai pflush thread block this signal
 */
void thread_block_alarm() {
	sigset_t set;

	sigemptyset(&set);
	sigaddset(&set, SIGALRM);

	pthread_sigmask(SIG_BLOCK, &set, NULL);
}

/*
 * thread_set_alarm: every user thread must register this signal
 */
int thread_set_alarm() {
	//return register_alarm(thread_alarm_handler);
	return 0;
}

#if 0
static void main_alarm_handler(int sig) {
	bonsai->desc->epoch ++;

	bonsai_flush((void*)bonsai->desc->epoch, sizeof(__le64), 1);

	bonsai_debug("bonsai epoch[%d]\n", bonsai->desc->epoch);
}

/*
 * epoch_init: main thread register this signal
 */
int epoch_init() {
	return register_alarm(main_alarm_handler);
}
#endif
