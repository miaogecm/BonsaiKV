#ifndef KVSTORE_CONFIG_H
#define KVSTORE_CONFIG_H

#define NUM_CPU         48
#define NUM_THREADS     48

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

#endif //KVSTORE_CONFIG_H
