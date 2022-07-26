#ifndef KVSTORE_CONFIG_H
#define KVSTORE_CONFIG_H

#define NUM_CPU         8
#define NUM_THREADS     8

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

#endif //KVSTORE_CONFIG_H
