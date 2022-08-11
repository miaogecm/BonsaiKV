#ifndef KVSTORE_CONFIG_H
#define KVSTORE_CONFIG_H

#define NUM_CPU         96
#define NUM_THREADS     96

#if 1
static int cpuseq[] = {
    0, 48, 1, 49, 2, 50, 3, 51, 4, 52, 5, 53, 6, 54, 7, 55, 8, 56, 9, 57, 10, 58, 11, 59, 12, 60, 13, 61, 14, 62, 15, 63, 16, 64, 17, 65, 18, 66, 19, 67, 20, 68, 21, 69, 22, 70, 23, 71, 24, 72, 25, 73, 26, 74, 27, 75, 28, 76, 29, 77, 30, 78, 31, 79, 32, 80, 33, 81, 34, 82, 35, 83, 36, 84, 37, 85, 38, 86, 39, 87, 40, 88, 41, 89, 42, 90, 43, 91, 44, 92, 45, 93, 46, 94, 47, 95
};
#else
static int cpuseq[] = {
    0, 48, 2, 50, 4, 52, 6, 54, 8, 56, 10, 58, 12, 60, 14, 62, 16, 64, 18, 66, 20, 68, 22, 70, 24, 72, 26, 74, 28, 76, 30, 78, 32, 80, 34, 82, 36, 84, 38, 86, 40, 88, 42, 90, 44, 92, 46, 94
};
#endif

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

struct pacman_config {
    int num_workers;
};

#endif //KVSTORE_CONFIG_H
