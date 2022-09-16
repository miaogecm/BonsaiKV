#include <time.h>
#include <stdlib.h>

static __thread unsigned int seed;
static __thread int seeded = 0;

static inline void thread_rand_init() {
    seed = time(NULL);
}

int gen_rand() {
    if (!seeded) {
        thread_rand_init();
        seeded = 1;
    }
    return rand_r(&seed);
}
