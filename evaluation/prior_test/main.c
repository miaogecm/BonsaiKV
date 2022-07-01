/*
 * Intel PMEM Performance Test
 */

#define _GNU_SOURCE

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "arch.h"
#include "pool.h"
#include "measure.h"

void run_test();

int main(int argc, char *argv[]) {
    run_test();
    return 0;
}
