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

#include "bptree.h"

#include "bench.h"

void kv_print(void* p) {

}

int main() {
	return bench("bptree", bp_init, bp_destory, bp_insert, NULL, bp_lookup, bp_scan);
}