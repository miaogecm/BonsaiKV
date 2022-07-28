#ifndef KVSTORE_CONFIG_H
#define KVSTORE_CONFIG_H

#define NUM_CPU         48
#define NUM_THREADS     48

static int cpuseq[] = {
    0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46,
    1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47
};

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

#endif //KVSTORE_CONFIG_H
