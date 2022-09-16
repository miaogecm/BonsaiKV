#ifndef RAND_H
#define RAND_H

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <assert.h>

#include "types.h"

int gen_rand();

/* rand u32 in [a, b] */
static inline u32 get_rand(u32 a, u32 b) {
    assert(a <= b);
    return gen_rand() % (b - a + 1) + a;
}

static inline u32 get_rand_except(u32 a, u32 b, u32 c) {
    u32 ret;
    while(1) {
        ret = get_rand(a, b);
        if (ret != c) {
            return ret;
        }
    }
}

static inline u32 get_nurand(u32 a, u32 x, u32 y, u32 c) {
    return (((get_rand(0, a) | get_rand(x, y)) + c) % (y - x + 1)) + x;
}

static inline double get_rand_lf(double a, double b) {
    double range = b - a;
    double div = RAND_MAX / range;
    return a + gen_rand() / div;
}

static inline void get_rand_str(char* str, u32 min_len, u32 max_len) {
    u32 len = get_rand(min_len, max_len - 1);
    int i, x;

    for (i = 0; i < len; i++) {
        x = get_rand(0, 61);
        if (x < 10) {
            str[i] = '0' + x;
        } else if (x < 36) {
            str[i] = 'A' + x - 10;
        } else {
            str[i] = 'a' + x - 36;
        }
    }
    str[len] = '\0';
}

static inline void get_rand_int_str(char* str, u32 min_len, u32 max_len) {
    u32 len = get_rand(min_len, max_len - 1);
    int i, x;

    for (i = 0; i < len; i++) {
        x = get_rand(0, 10);
        str[i] = '0' + x;
    }
    str[len] = '\0';
}

#define I_ID_A  8191
#define C_ID_A  1023

#define I_ID_C  I_ID_A
#define C_ID_C  C_ID_A

#define LAST_NAME_A         255
#define LAST_NAME_LOAD_C    200
#define LAST_NAME_RUN_C     100

#define get_w_id()      (get_rand(1, num_w))
#define get_d_id()      (get_rand(1, NUM_D))
#define get_ol_cnt()    (get_rand(MIN_O_OL, MAX_O_OL))
#define get_qlty()      (get_rand(MIN_OL_S, MAX_OL_S))
#define get_carrier()   (get_rand(MIN_CR, MAX_CR))
#define get_th()        (get_rand(MIN_TH, MAX_TH))

#define get_i_id()      (get_nurand(I_ID_A, 1, NUM_I, I_ID_C))
#define get_c_id()      (get_nurand(C_ID_A, 1, NUM_C, C_ID_C))

static const char part_name[10][6] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

static inline void get_name(char* last_name, int id) {
    last_name[0] = '\0';
    strcat(last_name, part_name[id / 100]);
    strcat(last_name, part_name[(id / 10) % 10]);
    strcat(last_name, part_name[id % 10]);
}

static inline void get_laod_name(char* last_name) {
    int id = get_nurand(LAST_NAME_A, 0, 999, LAST_NAME_RUN_C);
    get_name(last_name, id);
}

static inline void get_run_name(char* last_name) {
    int id = get_nurand(LAST_NAME_A, 0, 999, LAST_NAME_RUN_C);
    get_name(last_name, id);
}

#endif