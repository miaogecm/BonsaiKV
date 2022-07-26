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
#include "config.h"

double run_test(int num_worker);

int main(int argc, char *argv[]) {
    double time;
    char *path;
    FILE *out;
    int i, j;

    if (argc < 2) {
        printf("Usage: ./nvm_interleave_granularity <data_output_path>");
        exit(1);
    }

    path = argv[1];
    out = fopen(path, "w+");

    for (i = 0; i < ARRAY_LEN(num_workers); i++) {
        fprintf(out, "%d ", num_workers[i]);
    }

    fprintf(out, "\n%lu ", CONFIG_CHUNK_SIZE);
    for (j = 0; j < ARRAY_LEN(num_workers); j++) {
        time = run_test(num_workers[j]);
        fprintf(out, "%lf ", time);
    }

    fclose(out);

    return 0;
}