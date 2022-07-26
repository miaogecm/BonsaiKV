/*
 * Implementation of 1 transaction in TPC-C benchmark.
 * New-order
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "types.h"
#include "schema_tpcc.h"
#include "limit.h"
#include "rand.h"
#include "db.h"
#include "atomic.h"
#include "config.h"
#include "loader.h"
#include "runner.h"

static atomic_t h_pks[NUM_THREADS];

static int num_work, num_w;

static inline u64 get_timestamp() {
    static atomic64_t timestamp = ATOMIC64_INIT(10000);
    return atomic64_add_return(1, &timestamp);
}

static void __new_order(struct kvstore *kvstore, void *tcontext, u32 w_id, u32 d_id, u32 c_id, u32 ol_size,
                        u32 *w_arr, u32 *i_arr, u32 *q_arr) {
    struct warehouse __w;
    struct customer __c;
    struct district __d;
    struct order __o;
    struct neworder __no;
    struct stock __s;
    struct item __i;
    struct orderline __ol;

    double w_tax, c_discount;
    double d_tax;
    u32 o_id, all_local;
    double s_quantity, i_price;
    double s_remote_cnt, s_order_cnt, s_ytd;
    char s_dist[24];
    double ol_amount;

    int i;
    int col_arr[20];
    size_t size_arr[20];
    void* v_arr[20];

    set_w_k(__w.k, w_id);
    set_col_arr(col_arr, 1, e_w_tax);
    set_size_arr(size_arr, 1, W_FS(w_tax));
    set_v_arr(v_arr, 1, &w_tax);
    db_lookup(warehouse, __w.k, 1, col_arr, size_arr, v_arr);

    set_c_k(__c.k, w_id, d_id, c_id);
    set_col_arr(col_arr, 1, e_c_discount);
    set_size_arr(size_arr, 1, C_FS(c_discount));
    set_v_arr(v_arr, 1, &c_discount);
    db_lookup(customer, __c.k, 1, col_arr, size_arr, v_arr);

    set_d_k(__d.k, w_id, d_id);
    set_col_arr(col_arr, 2, e_d_tax, e_d_next_o_id);
    set_size_arr(size_arr, 2, D_FS(d_tax), D_FS(d_next_o_id));
    set_v_arr(v_arr, 2, &d_tax, &o_id);
    if(!db_lookup(district, __d.k, 2, col_arr, size_arr, v_arr)) {
        o_id++;
        set_col_arr(col_arr, 1, e_d_next_o_id);
        set_size_arr(size_arr, 1, D_FS(d_next_o_id));
        set_v_arr(v_arr, 1, &o_id); 
        db_update(district, __d.k, 1, col_arr, size_arr, v_arr);
    }

    all_local = 1;
    for (i = 0; i < ol_size; i++) {
        if (w_arr[i] != w_id) {
            all_local = 0;
            break;
        }
    }
    set_o_k(__o.k, w_id, d_id, o_id);
    set_o_v(__o.v, c_id, 0, 0, ol_size, all_local);
    db_insert(order, __o.k, __o.v, num_o_v, o_size_arr);

    set_no_k(__no.k, w_id, d_id, o_id);
    memset(&__no.v, 0, sizeof(__no.v));
    db_insert(neworder, __no.k, __no.v, num_no_v, no_size_arr);
    
    for (i = 0; i < ol_size; i++) {
        set_i_k(__i.k, i_arr[i]);
        set_col_arr(col_arr, 1, e_i_price);
        set_size_arr(size_arr, 1, I_FS(i_price));
        set_v_arr(v_arr, 1, &i_price); 
        db_lookup(item, __i.k, 1, col_arr, size_arr, v_arr);

        set_s_k(__s.k, w_id, i_arr[i]);
        set_col_arr(col_arr, 1, e_s_dist_01);
        set_size_arr(size_arr, 1, S_FS(s_dist_01));
        set_v_arr(v_arr, 1, s_dist); 
        db_lookup(stock, __s.k, 1, col_arr, size_arr, v_arr);

        set_s_k(__s.k, w_arr[i], i_arr[i]);
        set_col_arr(col_arr, 4, e_s_quantity, e_s_remote_cnt, e_s_order_cnt, e_s_ytd);
        set_size_arr(size_arr, 4, S_FS(s_quantity), S_FS(s_remote_cnt), S_FS(s_order_cnt), S_FS(s_ytd));
        set_v_arr(v_arr, 4, &s_quantity, &s_remote_cnt, &s_order_cnt, &s_ytd); 
        if(!db_lookup(stock, __s.k, 4, col_arr, size_arr, v_arr)) {
            s_quantity = (s_quantity >= q_arr[i] + 10) ? s_quantity - q_arr[i] : s_quantity + 91 - q_arr[i];
            if (w_arr[i] != w_id) s_remote_cnt++;
            s_order_cnt++;
            s_ytd += q_arr[i];
            db_update(stock, __s.k, 4, col_arr, size_arr, v_arr);
        }

        ol_amount = q_arr[i] * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
        set_ol_k(__ol.k, w_id, d_id, o_id, i);
        set_ol_v(__ol.v, i_arr[i], w_arr[i], get_timestamp(), q_arr[i], ol_amount, s_dist);
        db_insert(orderline, __ol.k, __ol.v, num_ol_v, ol_size_arr);
    }
}

void tpcc_new_order(struct kvstore *kvstore, void *tcontext, u32 w_id) {
    u32 d_id, c_id;
    u32 ol_cnt;
    u32 *w_arr, *i_arr, *q_arr;
    unsigned long size;
    int i;

    d_id = get_d_id();
    c_id = get_c_id();

    ol_cnt = get_ol_cnt();
    size = ol_cnt * sizeof(u32);
    w_arr = (u32*) malloc(size);
    i_arr = (u32*) malloc(size);
    q_arr = (u32*) malloc(size);
    
    for (i = 0; i < ol_cnt; i++) {
        w_arr[i] = get_rand(1, 100) <= LOCAL_W_PR ? w_id : get_rand_except(1, num_w, w_id);
        i_arr[i] = get_i_id();
        q_arr[i] = get_qlty();
    }
    
    __new_order(kvstore, tcontext, w_id, d_id, c_id, ol_cnt, w_arr, i_arr, q_arr);
}

void tpcc_load_warehouse(struct kvstore *kvstore, void *tcontext) {
    struct warehouse __w;
    
    char name[10];
    char street_1[20];
	char street_2[20];
	char city[20];
	char state[3];
	char zip[9];
    int i;

    for (i = 1; i <= num_w; i++) {
        set_w_k(__w.k, i);
        get_rand_str(name, 6, 10);
        get_rand_str(street_1, 10, 20);
        get_rand_str(street_2, 10, 20);
        get_rand_str(city, 10, 20);
        get_rand_str(state, 1, 2);
        get_rand_int_str(zip, 8, 9);
        set_w_v(__w.v, name, street_1, street_2, city, state, zip, 
                get_rand_lf(0.1, 0.2), 3000000);
        db_insert(warehouse, __w.k, __w.v, num_w_v, w_size_arr);
    }
}

void tpcc_load_district(struct kvstore *kvstore, void *tcontext, u32 w_id) {
    struct district __d;
    
    char name[10];
    char street_1[20];
	char street_2[20];
	char city[20];
	char state[3];
	char zip[9];
    int i;
    
    for (i = 1; i <= NUM_D; i++) {
        set_d_k(__d.k, w_id, i);
        get_rand_str(name, 6, 10);
        get_rand_str(street_1, 10, 20);
        get_rand_str(street_2, 10, 20);
        get_rand_str(city, 10, 20);
        get_rand_str(state, 2, 3);
        get_rand_int_str(zip, 8, 9);
        set_d_v(__d.v, name, street_1, street_2, city, state, zip, 
                get_rand_lf(0.1, 0.2), 3000000, 3001);
        db_insert(district, __d.k, __d.v, num_d_v, d_size_arr);
    }
}

void tpcc_load_customer(struct kvstore *kvstore, void *tcontext, u32 t_id, u32 w_id, u32 d_id) {
    int i;
    struct customer __c;
    struct history __h;
    struct customer_info __ci;

    char c_last[16];
    char c_first[16];
    char street_1[20];
	char street_2[20];
	char city[20];
	char state[3];
	char zip[9];
    char c_phone[16];
    char c_credit[3];
    char c_data[500];
    char h_data[24];
    u32 h_pk;

    for (i = 1; i <= NUM_C; i++) {
        set_c_k(__c.k, w_id, d_id, i);
        if (i <= 1000) {
            get_name(c_last, i - 1);
        } else {
            get_laod_name(c_last);
        }
        get_rand_str(c_first, 8, 16);
        if (get_rand(1, 100) <= GOOD_CR_PR) {
            strcpy(c_credit, "GC");
        } else {
            strcpy(c_credit, "BC");
        }
        get_rand_str(street_1, 10, 20);
        get_rand_str(street_2, 10, 20);
        get_rand_str(city, 10, 20);
        get_rand_str(state, 2, 3);
        get_rand_int_str(zip, 8, 9);
        get_rand_int_str(c_phone, 15, 16);
        get_rand_str(c_data, 300, 500);
        set_c_v(__c.v, c_first, "OE", c_last, street_1, street_2, city,
                state, zip, c_phone, 0, c_credit, 50000, get_rand_lf(0, 0.5),
                -10, 1, 0, 0, c_data);
        db_insert(customer, __c.k, __c.v, num_c_v, c_size_arr);
        
        set_c_i_k(__ci.k, w_id, d_id, c_last, c_first);
        set_c_i_v(__ci.v, i);
        db_insert(customer_info, __ci.k, __ci.v, num_ci_v, ci_size_arr);

        h_pk = atomic_add_return(1, &h_pks[t_id]);
        set_h_k(__h.k, t_id, h_pk);
        get_rand_str(h_data, 12, 24);
        set_h_v(__h.v, i, d_id, w_id, d_id, w_id, get_timestamp(), 10, h_data);
        db_insert(history, __h.k, __h.v, num_h_v, h_size_arr);
    }
}

void tpcc_load_order(struct kvstore *kvstore, void *tcontext, u32 w_id, u32 d_id) {
    struct order __o;
    struct orderline __ol;
    struct neworder __no;

    u32 car_id, i_id;
    double ol_cnt, ol_amount;
    char dist_info[24];
    int i, j;

    for (i = 1; i <= NUM_O; i++) {
        if (i < 2101) {
            car_id = get_rand(1, 10);
        } else {
            set_no_k(__no.k, w_id, d_id, i);
            set_no_v(__no.v);
            db_insert(neworder, __no.k, __no.v, num_no_v, no_size_arr);
            car_id = 0;
        }
        ol_cnt = get_rand(MIN_O_OL, MAX_O_OL);
        set_o_k(__o.k, w_id, d_id, i);
        set_o_v(__o.v, i, 0, car_id, ol_cnt, 1);
        db_insert(order, __o.k, __o.v, num_o_v, o_size_arr);

        for (j = 1; j <= ol_cnt; j++) {
            if (i < 2101) {
                ol_amount = 0;
            } else {
                ol_amount = get_rand_lf(0.01, 9999.99);
            }
            i_id = get_rand(1, NUM_I);
            set_ol_k(__ol.k, w_id, d_id, i, j);
            get_rand_str(dist_info, 23, 24);
            set_ol_v(__ol.v, get_rand(1, NUM_I), w_id, get_timestamp(), 5, ol_amount, dist_info);
            db_insert(orderline, __ol.k, __ol.v, num_ol_v, ol_size_arr);
        }
    }
}

void tpcc_load_item(struct kvstore *kvstore, void *tcontext) {
    struct item __i;
    
    char i_data[50], __i_data[50];
    char name[24];
    int i;
    
    for (i = 1; i <= NUM_I; i++) {
        set_i_k(__i.k, i);
        get_rand_str(name, 14, 24);
        if (get_rand(1, 100) <= ORG_PR) {
            strcpy(i_data, "ORIGINAL");
            get_rand_str(__i_data, 25, 40);
            strcat(i_data, __i_data);
        } else {
            get_rand_str(i_data, 25, 50);
        }
        set_i_v(__i.v, get_rand(1, NUM_I), name, get_rand_lf(1, 100), i_data);
        db_insert(item, __i.k, __i.v, num_i_v, i_size_arr);
    }
}

void tpcc_load_stock(struct kvstore *kvstore, void *tcontext, u32 w_id) {
    struct stock __s;
    
    char s_data[50], __s_data[50];
    char s_dist[10][24];
    int i, j;
    
    for (i = 1; i <= NUM_S; i++) {
        set_s_k(__s.k, w_id, i);
        for (j = 0; j < 10; j++) {
            get_rand_str(s_dist[j], 23, 24);
        }
        if (get_rand(1, 100) <= ORG_PR) {
            strcpy(s_data, "ORIGINAL");
            get_rand_str(__s_data, 25, 40);
            strcat(s_data, __s_data);
        } else {
            get_rand_str(s_data, 25, 50);
        }
        set_s_v(__s.v, get_rand(10, 100), s_dist, 0, 0, 0, s_data);
        db_insert(stock, __s.k, __s.v, num_s_v, s_size_arr);
    }
}

static const char *tpcc_load_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    int st, ed, i, j;

    if (id == 0) {
        tpcc_load_item(kvstore, tcontext);
        tpcc_load_warehouse(kvstore, tcontext);
    }

    st = 1.0 * id / NUM_THREADS * (num_w + 1);
    ed = 1.0 * (id + 1) / NUM_THREADS * (num_w + 1);
    for (i = st; i < ed; i++) {
        tpcc_load_stock(kvstore, tcontext, i);
        tpcc_load_district(kvstore, tcontext, i);
        for (j = 1; j <= NUM_D; j++) {
            tpcc_load_customer(kvstore, tcontext, id, i, j);
            tpcc_load_order(kvstore, tcontext, i, j);
        }
    }

    return "load";
}

int tpcc_work(struct kvstore *kvstore, void *tcontext, u32 t_id, u32 w_id) {
    kvstore->kv_txn_begin(tcontext);
    tpcc_new_order(kvstore, tcontext, w_id);
    kvstore->kv_txn_commit(tcontext);
    return 0;
}

static const char *tpcc_txn_stage_fun(struct kvstore *kvstore, void *tcontext, int id) {
    int num_works_left;

    num_works_left = num_work / NUM_THREADS;
    if (id == 0) {
        num_works_left += num_work % NUM_THREADS;
    }

    while(num_works_left--) {
		tpcc_work(kvstore, tcontext, id, get_w_id());
    }

    return "txn";
}

void run_tpcc(const char *kvlib, int num_w_, int num_work_) {
    const char *(*stage_func[])(struct kvstore *, void *, int) = { tpcc_load_stage_fun, tpcc_txn_stage_fun };
    struct kvstore kvstore;
    const char *engine;
    void *conf = NULL;
    num_w = num_w_;
    num_work = num_work_;
    load_kvstore(&kvstore, kvlib);
    engine = kvstore.kv_engine();
    if (!strcmp(engine, "bonsai")) {
        struct bonsai_config *c = malloc(sizeof(*c));
        c->stm_support = 1;
        conf = c;
    }
    run_kvstore(&kvstore, conf, 2, stage_func);
    free(conf);
}
