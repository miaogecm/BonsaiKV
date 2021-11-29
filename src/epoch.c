#include <signal.h>
#include <stdio.h>
#include <sys/time.h>

#include "thread.h"

static void main_alarm_handler(int sig) {
	LOG(bonsai)->epoch ++;

	printf("bonsai epoch[%d]\n", LOG(bonsai)->epoch);
}

static void thread_alarm_handler(int sig) {
	struct log_region *region = &LOG(bonsai)->region[get_cpu()];
	
	/* persist it */
	clfush(region->block, sizeof(struct oplog_blk));
	mfence();
}

int epoch_init() {
	struct itimerval value;
	struct sigaction sa;
	int err = 0;

	memset(&value, sizeof(struct itimerval));
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
