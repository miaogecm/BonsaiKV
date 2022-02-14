/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 
 * A toy key-value store
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
#include <sys/time.h>

#include "ffwrapper.h"

#include "bench.h"

void kv_print(void* p) {

}

int main() {
	return bench("fast_fair", ff_init, ff_destory, ff_insert, ff_remove, ff_lookup, ff_scan);
}
