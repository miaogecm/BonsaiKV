#define _GNU_SOURCE

#include <sched.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

static inline void bind_to_cpu(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind cpu failed\n");
    }
}

struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
};

void *kv_create_context(void *config);
void kv_destroy_context(void *context);
void kv_start_test(void *context);
void kv_stop_test(void *context);
void *kv_thread_create_context(void *context, int id);
void kv_thread_destroy_context(void *tcontext);
void kv_thread_start_test(void *tcontext);
void kv_thread_stop_test(void *tcontext);
int kv_put(void *tcontext, void *key, size_t key_len, void *val, size_t val_len);
int kv_del(void *tcontext, void *key, size_t key_len);
int kv_get(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len);

int main() {
    struct bonsai_config cfg = { 1, (int []) { 0 } };
    void *context, *thread_context;
    char value[8];
    size_t len;
    int ret;

    bind_to_cpu(0);

    context = kv_create_context(&cfg);
    thread_context = kv_thread_create_context(context, 0);
    kv_start_test(context);
    kv_thread_start_test(thread_context);

    kv_put(thread_context, "abcdefgh", 8, "hellowld", 8);

    ret = kv_get(thread_context, "abcdefgh", 8, value, &len);
    printf("got: %d %.*s %zu\n", ret, len, value, len);

    kv_thread_stop_test(thread_context);
    kv_stop_test(context);
    kv_thread_destroy_context(thread_context);
    kv_destroy_context(context);

    return 0;
}
