#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "common.h"
#include "hash_set.h"

#if 1
#ifndef MAXN
#define MAXN 3
#endif

pval_t a[MAXN];
struct hash_set hs;

void data_gen() {
    int i;

    for (i = 1; i < MAXN; i++)
        a[i] = i;
    
    #ifdef RAND
    #if (RAND_MAX < MAXN)
        #define RAND32() (rand() * rand())
    #else
        #define RAND32() rand()
    #endif

    for (i = 1; i < MAXN; i++) {
        int swap_pos = RAND32() % (MAXN - i) + i;
        sl_key_t temp = a[i];
        a[i] = a[swap_pos];
        a[swap_pos] = temp;
    }
    #endif
}

int main() {
    pval_t** v;
    int i;

    data_gen();
    hs_init(&hs);

    for (i = 1; i < MAXN; i++) {
        assert(hs_insert(&hs, 0, a[i], a[i]) == 0);
    }

    for (i = 1; i < MAXN; i++) {
        v = hs_lookup(&hs, 0, a[i]);
        assert(v && *v == a[i]);
    }

    for (i = 1; i < MAXN; i++) {
        assert(hs_remove(&hs, 0, a[i]) == 0);
    }

    // for (i = 1; i < MAXN; i++) {
    //     v = hs_lookup(&hs, 0, a[i]);
    //     assert(v == NULL);
    // }

	return 0;
}
#else
skiplist_t* sl;

#ifndef MAXN
#define MAXN 5000001
#endif

#ifndef THREAD_NUM
#define THREAD_NUM 1
#endif

#ifndef TIME_LIMIT
#define TIME_LIMIT 3
#endif

sl_key_t a[THREAD_NUM][MAXN];
entry_t res[MAXN];

int time_out = 0;

typedef struct worker_s {
    int id;
    sl_value_t return_val;
    double t;
}worker_t;

worker_t* insert_test(worker_t* wk) {
    int id = wk->id;
    struct timeval begin_t, end_t;
    gettimeofday(&begin_t, NULL);
    set_affinity(2 * id + 2);
    int i;
    uint64_t v;
    for (i = 1; i < MAXN && !time_out; i++)
        h_sl_insert(sl, a[id][i], a[id][i], id);
    
    gettimeofday(&end_t, NULL);

    wk->t = end_t.tv_sec - begin_t.tv_sec + 1.0 * (end_t.tv_usec - begin_t.tv_usec) / 1000000;
    wk->return_val = i - 1;
    return wk;
}

worker_t* lookup_test(worker_t* wk) {
    int id = wk->id;

    struct timeval begin_t, end_t;
    gettimeofday(&begin_t, NULL);
    
    set_affinity(2 * id + 2);
    int i;
    uint64_t v;
    int64_t turn = -1;
    while(!time_out) {
        turn++;
        i = 1;
        for (; i < MAXN && !time_out; i++)
            h_sl_lookup(sl, a[id][i], &v, id);
    }

    gettimeofday(&end_t, NULL);
    wk->t = end_t.tv_sec - begin_t.tv_sec + 1.0 * (end_t.tv_usec - begin_t.tv_usec) / 1000000;

    wk->return_val = turn * (MAXN - 1) + i - 1;
    return wk; 
}

worker_t* remove_test(worker_t* wk) {
    int id = wk->id;
    struct timeval begin_t, end_t;
    gettimeofday(&begin_t, NULL);
    set_affinity(2 * id + 2);
    int i;
    uint64_t v;
    for (i = 1; i < MAXN && !time_out; i++)
        h_sl_remove(sl, a[id][i], id);
    
    gettimeofday(&end_t, NULL);

    wk->t = end_t.tv_sec - begin_t.tv_sec + 1.0 * (end_t.tv_usec - begin_t.tv_usec) / 1000000;
    wk->return_val = i - 1;
    return wk;
}

static inline void sig_alarm() {
    time_out = 1;
}

int main() {
    signal(SIGALRM, sig_alarm);

    init_pmdk();
	sl = init_h_sl();

    uint64_t v;
    
    double t;
    struct timeval begin_t, end_t;

    pthread_t threads[THREAD_NUM];
    int i, j;

    for (i = 0; i < THREAD_NUM; i++)
        for (j = 1; j < MAXN; j++)
            a[i][j] = j;
    
    while(1){    
#ifdef RAND
    srand(time(0));
    #if (RAND_MAX < MAXN)
        #define RAND32() (rand() * rand())
    #else
        #define RAND32() rand()
    #endif
    for (i = 0; i < THREAD_NUM; i++) {
        for (j = 1; j < MAXN; j++) {
            int swap_pos = RAND32() % (MAXN - j) + j;
            uint64_t temp = a[i][j];
            a[i][j] = a[i][swap_pos];
            a[i][swap_pos] = temp;
        }
    }
#endif
        double tot = 0;
        // printf("----------print----------\n");
        // h_sl_print(sl);
#if 1
        printf("----------insert----------\n");
        time_out = 0;
        alarm(TIME_LIMIT);
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk = (worker_t*) malloc(sizeof(worker_t));
            wk->id = i;
            if (pthread_create(&threads[i], NULL, (void*)insert_test, wk) != 0) {
                printf("pthread_create error.");
                exit(1);
            }
        }
        
        tot = 0;
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk;
            pthread_join(threads[i], (void**)(&wk));
            int64_t res = wk->return_val;
            if (res == MAXN - 1)
                res = 1.0 * res / wk->t * 3;
            tot += res;
            free(wk);
        }
        printf("%.0lf Kips\n", tot/1000/(TIME_LIMIT));

        // h_sl_print(sl);
#endif
        for (i = 1; i < MAXN; i++)
            h_sl_insert(sl, i, i, 0);
#if 1       
        printf("----------lookup----------\n");
        time_out = 0;
        alarm(TIME_LIMIT);
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk = (worker_t*) malloc(sizeof(worker_t));
            wk->id = i;
            if (pthread_create(&threads[i], NULL, (void*)lookup_test, wk) != 0) {
                printf("pthread_create error.");
                assert(0);
            }
        }
        
        tot = 0;
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk;
            pthread_join(threads[i], (void**)(&wk));
            int64_t res = wk->return_val;
            tot += res;
            free(wk);
        }
        printf("%.0lf Kips\n", tot/1000/(TIME_LIMIT));
#endif
        printf("----------remove----------\n");
        time_out = 0;
        alarm(TIME_LIMIT);
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk = (worker_t*) malloc(sizeof(worker_t));
            wk->id = i;
            if (pthread_create(&threads[i], NULL, (void*)remove_test, wk) != 0) {
                printf("pthread_create error.");
                exit(1);
            }
        }
        
        tot = 0;
        for (i = 0; i < THREAD_NUM; i++) {
            worker_t* wk;
            pthread_join(threads[i], (void**)(&wk));
            int64_t res = wk->return_val;
            if (res == MAXN - 1)
                res = 1.0 * res / wk->t * 3;
            tot += res;
            free(wk);
        }
        printf("%.0lf Kips\n", tot/1000/(TIME_LIMIT));

        free_h_sl(sl);
        // h_sl_print(sl);

        break;
    }
    uninit_pmdk();

    return 0;
}
#endif