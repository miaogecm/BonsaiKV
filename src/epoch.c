/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#include <signal.h>
#include <stdio.h>
#include <sys/time.h>

#include "thread.h"
#include "region.h"
#include "oplog.h"
#include "common.h"
#include "bonsai.h"
#include "epoch.h"

extern struct bonsai_info* bonsai;

static void main_alarm_handler(int sig) {
	struct log_layer* layer = LOG(bonsai);
	layer->epoch ++;

	printf("bonsai epoch[%d]\n", layer->epoch);
}

static void thread_alarm_handler(int sig) {
	struct log_layer* layer = LOG(bonsai);
	struct log_region *region = &layer->region[get_cpu()];
	
	/* persist it */
	bonsai_flush(region->curr_blk, sizeof(struct oplog_blk), 1);
}

int epoch_init() {
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
	sa.sa_handler = main_alarm_handler;
	if (sigaction(SIGALRM, &sa, NULL) == -1) {
		err = -ESIGNO;
	}

	return 0;
}
