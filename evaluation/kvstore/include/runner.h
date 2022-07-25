#ifndef KVSTORE_RUNNER_H
#define KVSTORE_RUNNER_H

struct kvstore;

void run_kvstore(struct kvstore *kv, void *conf, int nr_stage,
                 const char *(*stage_func[])(struct kvstore *, void *, int));

#endif //KVSTORE_RUNNER_H
