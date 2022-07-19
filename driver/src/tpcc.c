/*
 * Implementation of five transactions in TPC-C benchmark.
 * New-order, Payment, Order-status, Deliver, Stock-level
 */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
#pragma GCC diagnostic ignored "-Wunused-variable"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "types.h"
#include "schema.h"
#include "workload.h"
#include "limit.h"
#include "rand.h"
#include "bench.h"
#include "op.h"
#include "atomic.h"
#include "transcat.h"

static inline u64 get_timestamp() {
    static atomic64_t timestamp = ATOMIC64_INIT(10000);
    return atomic64_add_return(1, &timestamp);
}

static void __new_order(u32 w_id, u32 d_id, u32 c_id, u32 ol_size, 
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
    int col_arr[20], size_arr[20];
    void* v_arr[20];

    set_w_k(__w.k, w_id);
    set_col_arr(col_arr, 1, e_w_tax);
    set_size_arr(size_arr, 1, W_FS(w_tax));
    set_v_arr(v_arr, 1, &w_tax);
    bench_lookup(warehouse, __w.k, 1, col_arr, size_arr, v_arr);

    set_c_k(__c.k, w_id, d_id, c_id);
    set_col_arr(col_arr, 1, e_c_discount);
    set_size_arr(size_arr, 1, C_FS(c_discount));
    set_v_arr(v_arr, 1, &c_discount);
    bench_lookup(customer, __c.k, 1, col_arr, size_arr, v_arr);

    set_d_k(__d.k, w_id, d_id);
    set_col_arr(col_arr, 2, e_d_tax, e_d_next_o_id);
    set_size_arr(size_arr, 2, D_FS(d_tax), D_FS(d_next_o_id));
    set_v_arr(v_arr, 2, &d_tax, &o_id);
    if(bench_lookup(district, __d.k, 2, col_arr, size_arr, v_arr)) {
        o_id++;
        set_col_arr(col_arr, 1, e_d_next_o_id);
        set_size_arr(size_arr, 1, D_FS(d_next_o_id));
        set_v_arr(v_arr, 1, &o_id); 
        bench_update(district, __d.k, 1, col_arr, size_arr, v_arr);
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
    bench_insert(order, __o.k, __o.v, num_o_v, o_size_arr);

    set_no_k(__no.k, w_id, d_id, o_id);
    memset(&__no.v, 0, sizeof(__no.v));
    bench_insert(neworder, __no.k, __no.v, num_no_v, no_size_arr);
    
    for (i = 0; i < ol_size; i++) {
        set_i_k(__i.k, i_arr[i]);
        set_col_arr(col_arr, 1, e_i_price);
        set_size_arr(size_arr, 1, I_FS(i_price));
        set_v_arr(v_arr, 1, &i_price); 
        bench_lookup(item, __i.k, 1, col_arr, size_arr, v_arr);

        set_s_k(__s.k, w_id, i_arr[i]);
        set_col_arr(col_arr, 1, e_s_dist_01);
        set_size_arr(size_arr, 1, S_FS(s_dist_01));
        set_v_arr(v_arr, 1, s_dist); 
        bench_lookup(stock, __s.k, 1, col_arr, size_arr, v_arr);

        set_s_k(__s.k, w_arr[i], i_arr[i]);
        set_col_arr(col_arr, 4, e_s_quantity, e_s_remote_cnt, e_s_order_cnt, e_s_ytd);
        set_size_arr(size_arr, 4, S_FS(s_quantity), S_FS(s_remote_cnt), S_FS(s_order_cnt), S_FS(s_ytd));
        set_v_arr(v_arr, 4, &s_quantity, &s_remote_cnt, &s_order_cnt, &s_ytd); 
        if(bench_lookup(stock, __s.k, 4, col_arr, size_arr, v_arr)) {
            s_quantity = (s_quantity >= q_arr[i] + 10) ? s_quantity - q_arr[i] : s_quantity + 91 - q_arr[i];
            if (w_arr[i] != w_id) s_remote_cnt++;
            s_order_cnt++;
            s_ytd += q_arr[i];
            bench_update(stock, __s.k, 4, col_arr, size_arr, v_arr);
        }

        ol_amount = q_arr[i] * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
        set_ol_k(__ol.k, w_id, d_id, o_id, i);
        set_ol_v(__ol.v, i_arr[i], w_arr[i], get_timestamp(), q_arr[i], ol_amount, s_dist);
        bench_insert(orderline, __ol.k, __ol.v, num_ol_v, ol_size_arr);
    }
}

void new_order(u32 w_id) {
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
    
    __new_order(w_id, d_id, c_id, ol_cnt, w_arr, i_arr, q_arr);
}

static void __payment_by_c_id(u32 t_id, u32 w_id, u32 d_id, u32 c_w_id, u32 c_d_id, u32 c_id, double h_amount, u64 h_date) {
    struct warehouse __w;
    struct district __d;
    struct customer __c;
    struct history __h;

    char w_name[10];
    char w_street_1[20];
    char w_street_2[20];
    char w_city[20];
    char w_state[3];
    char w_zip[9];
    double w_tax;
    double w_ytd;

    char d_name[10];
    char d_street_1[20];
    char d_street_2[20];
    char d_city[20];
    char d_state[3];
    char d_zip[9];
    double d_tax;
    double d_ytd;

    char c_data[500];
    char c_credit[3];
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    char   c_new_data[500];

    char h_new_data[24];
    u32 h_pk;

    int col_arr[20], size_arr[20];
    void* v_arr[20];

    set_w_k(__w.k, w_id);
    set_col_arr2(col_arr, num_w_v);
    set_v_arr(v_arr, 8, w_name, w_street_1, w_street_2, w_city,
            w_state, w_zip, &w_tax, &w_ytd);
    if(bench_lookup(warehouse, __w.k, 8, col_arr, w_size_arr, v_arr)) {
        w_ytd += h_amount;
        set_col_arr(col_arr, 1, e_w_ytd);
        set_size_arr(size_arr, 1, W_FS(w_ytd));
        set_v_arr(v_arr, 1, &w_ytd);
        bench_update(warehouse, __w.k, 1, col_arr, size_arr, v_arr);
    }
    
    set_d_k(__d.k, w_id, d_id);
    set_col_arr2(col_arr, num_d_v);
    set_v_arr(v_arr, 8, d_name, d_street_1, d_street_2, d_city,
            d_state, d_zip, &d_tax, &w_ytd);
    if(bench_lookup(district, __d.k, 8, col_arr, d_size_arr, v_arr)) {
        d_ytd += h_amount;
        set_col_arr(col_arr, 1, e_d_ytd);
        set_size_arr(size_arr, 1, D_FS(d_ytd));
        set_v_arr(v_arr, 1, &d_ytd);
        bench_update(district, __d.k, 1, col_arr, size_arr, v_arr);
    }

    set_c_k(__c.k, w_id, c_d_id, c_id);
    set_col_arr(col_arr, 5, e_c_data, e_c_credit, e_c_balance, e_c_ytd_payment, e_c_payment_cnt);
    set_size_arr(size_arr, 5, C_FS(c_data), C_FS(c_credit), C_FS(c_balance), C_FS(c_ytd_payment), C_FS(c_payment_cnt));
    set_v_arr(v_arr, 5, c_data, c_credit, &c_balance, &c_ytd_payment, &c_payment_cnt);
    if(bench_lookup(customer, __c.k, 5, col_arr, size_arr, v_arr)) {
        c_balance -= h_amount;
        c_ytd_payment += h_amount;
        c_payment_cnt++;

        if (strcmp(c_credit, "BC") == 0) {
            snprintf(c_data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", 
                    c_id, c_d_id, c_w_id, d_id, w_id, h_amount, h_date, w_name, d_name, c_data);
        }

        set_col_arr(col_arr, 4, e_c_data, e_c_balance, e_c_ytd_payment, e_c_payment_cnt);
        set_size_arr(size_arr, 4, C_FS(c_data), C_FS(c_balance), C_FS(c_ytd_payment), C_FS(c_payment_cnt));
        set_v_arr(v_arr, 4, c_data, &c_balance, &c_ytd_payment, &c_payment_cnt);
        bench_update(customer, __c.k, 4, col_arr, size_arr, v_arr);
    }
    
    strcpy(h_new_data, w_name);
    strcat(h_new_data, " ");
    strcat(h_new_data, d_name);
    h_pk = atomic_add_return(1, &bench.h_pk[t_id]);
    set_h_k(__h.k, t_id, h_pk);
    set_h_v(__h.v, c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_new_data);
    bench_insert(history, __h.k, __h.v, num_h_v, h_size_arr);
}

static void __payment_by_name(u32 t_id, u32 w_id, u32 d_id, u32 c_w_id, u32 c_d_id, char* c_last, double h_amount, u64 h_date) {
    struct customer_info __ci1, __ci2;
    
    u32 c_id;
    size_t k_arr[100000], v_arrs[1][100000];
    int arr_size;
    int col_arr[20];

    set_c_i_k(__ci1.k, w_id, d_id, c_last, "");
    set_c_i_k(__ci2.k, w_id, d_id, c_last, "~");
    set_col_arr(col_arr, 1, e_ci_c_id);
    arr_size = bench_scan(customer_info, __ci1.k, __ci2.k, 1, col_arr, k_arr, v_arrs);
    if (arr_size) {
        c_id = *(u32*) v_arrs[0][arr_size / 2];
        __payment_by_c_id(t_id, w_id, d_id, c_w_id, c_d_id, c_id, h_amount, h_date);
    }
}

void payment(u32 t_id, u32 w_id) {
    u32 d_id = get_d_id();
    u32 c_w_id;
    u32 c_d_id;
    double h_amount;
    u64 h_date;
    u32 c_id;
    char c_last[16];

    if (get_rand(1, 100) <= LOCAL_CW_PR) {
        c_w_id = w_id;
        c_d_id = d_id;
    } else {
        c_w_id = get_rand_except(1, NUM_D, w_id);
        c_d_id = get_d_id();
    }
    h_amount = get_rand_lf(MIN_AMOUNT, MAX_AMOUNT);
    h_date = get_timestamp();

    if (get_rand(1, 100) <= USE_C_ID_PR) {
        c_id = get_c_id();
        __payment_by_c_id(t_id, w_id, d_id, c_w_id, c_d_id, c_id, h_amount, h_date);
    } else {
        get_run_name(c_last);
        __payment_by_name(t_id, w_id, d_id, c_w_id, c_d_id, c_last, h_amount, h_date);
    }
}

static void __delivery(u32 w_id, u32 carrier_id, u64 d_date) {
    struct neworder __no1, __no2;
    struct order __o;
    struct orderline __ol;
    struct customer __c;

    size_t k_arr[100000], v_arrs[2][100000];
    int arr_size;
    u32 d_id, o_id;
    u32 ol_cnt, c_id;
    double ol_amount, ol_tot = 0;
    double c_balance, c_delivery_cnt;

    int i;
    int col_arr[20], size_arr[20];
    void* v_arr[20];

    for (d_id = 1; d_id <= NUM_D; d_id++) {
        set_no_k(__no1.k, w_id, d_id, 0);
        set_no_k(__no2.k, w_id, d_id, UINT32_MAX);
        set_col_arr(col_arr, 1, 0);
        arr_size = bench_scan(neworder, __no1.k, __no2.k, 1, col_arr, k_arr, v_arrs);
        if (arr_size) {
            o_id = ((struct neworder_k*) k_arr[0])->no_o_id;
            
            set_o_k(__o.k, w_id, d_id, o_id);
            set_col_arr(col_arr, 2, e_o_c_id, e_o_ol_cnt);
            arr_size = bench_scan(order, __o.k, __o.k, 2, col_arr, k_arr, v_arrs);
            if (arr_size) {
                c_id = *(u32*) v_arrs[0][0];
                ol_cnt = *(u32*) v_arrs[1][0];
                set_col_arr(col_arr, 1, e_o_carrier_id);
                set_size_arr(size_arr, 1, O_FS(o_carrier_id));
                set_v_arr(v_arr, 1, &carrier_id);
                bench_update(order, __o.k, 1, col_arr, size_arr, v_arr);

                set_ol_k(__ol.k, w_id, d_id, o_id, ol_cnt);
                set_col_arr(col_arr, 1, 0);
                arr_size = bench_scan(orderline, __ol.k, __ol.k, 1, col_arr, k_arr, v_arrs);
                if (arr_size) {
                    ol_tot = 0;
                    for (i = 1; i <= ol_cnt; i++) {
                        set_ol_k(__ol.k, w_id, d_id, o_id, i);
                        set_col_arr(col_arr, 1, e_ol_amount);
                        set_size_arr(size_arr, 1, OL_FS(ol_amount));
                        set_v_arr(v_arr, 1, &ol_amount);
                        if(bench_lookup(orderline, __ol.k, 1, col_arr, size_arr, v_arr)) {
                            ol_tot += ol_amount;
                            set_col_arr(col_arr, 1, e_ol_delivery_d);
                            set_size_arr(size_arr, 1, OL_FS(ol_delivery_d));
                            set_v_arr(v_arr, 1, &d_date);
                            bench_update(orderline, __ol.k, 1, col_arr, size_arr, v_arr);
                        }
                    }

                    set_c_k(__c.k, w_id, d_id, c_id);
                    set_col_arr(col_arr, 2, e_c_balance, e_c_delivery_cnt);
                    set_size_arr(size_arr, 2, C_FS(c_balance), C_FS(c_delivery_cnt));
                    set_v_arr(v_arr, 2, &c_balance, &c_delivery_cnt);
                    if(bench_lookup(customer, __c.k, 2, col_arr, size_arr, v_arr)) {
                        c_balance += ol_tot;
                        c_delivery_cnt++;
                        bench_update(customer, __c.k, 2, col_arr, size_arr, v_arr);
                    }
                }
            } 
        }
    }
}

void delivery(u32 w_id) {
    u32 carrier_id = get_carrier();
    __delivery(w_id, carrier_id, get_timestamp());
}

static void __stock_level(u32 w_id, u32 d_id, u32 threshold) {
    struct district __d;
    struct stock __s;
    struct orderline __ol1, __ol2;

    u32 o_id;
    size_t k_arr[100000], v_arrs[1][100000];
    u32 i_id;
    int arr_size;
    int i;
    u32 count = 0;
    double s_quantity;
    int col_arr[20], size_arr[20];
    void* v_arr[20];

    set_d_k(__d.k, w_id, d_id);
    set_col_arr(col_arr, 1, e_d_next_o_id);
    set_size_arr(size_arr, 1, D_FS(d_next_o_id));
    set_v_arr(v_arr, 1, &o_id);
    bench_lookup(district, __d.k, 2, col_arr, size_arr, v_arr);
    
    set_ol_k(__ol1.k, w_id, d_id, o_id - 20, 0);
    set_ol_k(__ol2.k, w_id, d_id, o_id - 1, UINT32_MAX);
    set_col_arr(col_arr, 1, e_s_quantity);
    arr_size = bench_scan(orderline, __ol1.k, __ol2.k, 1, col_arr, k_arr, v_arrs);
    
    for (i = 0; i < arr_size; i++) {
        i_id = *(u32*) v_arrs[0][i];
        set_s_k(__s.k, w_id, i_id);
        set_col_arr(col_arr, 1, e_s_quantity);
        set_size_arr(size_arr, 1, S_FS(s_quantity));
        set_v_arr(v_arr, 1, &s_quantity);
        if(bench_lookup(stock, __s.k, 1, col_arr, size_arr, v_arr)) {
            if (s_quantity < threshold) count++;
        }
    }
}

void stock_level(u32 w_id) {
    u32 d_id = get_d_id();
    u32 threshold = get_th();
    __stock_level(w_id, d_id, threshold);
}

static void __order_status_by_c_id(u32 w_id, u32 d_id, u32 c_id) {
    struct customer __c;
    struct order __o1, __o2;
    struct orderline __ol1, __ol2;

    size_t k_arr[100000], v_arrs[6][100000];
    int arr_size;

    char c_first[16];
    char c_middle[3];
    char c_last[16];
    double c_balance;
    u32 o_id;
    
    u64 o_ent_d;
    u32 o_car_id;

    u32 ol_i_id;
    u32 ol_supply_w_id;
    u64 ol_delivery_d;
    double ol_quantity;
    double ol_amount;

    int col_arr[20], size_arr[20];
    void* v_arr[20];

    set_c_k(__c.k, w_id, d_id, c_id);
    set_col_arr(col_arr, 4, e_c_first, e_c_middle, e_c_last, e_c_balance);
    set_size_arr(size_arr, 4, C_FS(c_first), C_FS(c_middle), C_FS(c_last), C_FS(c_balance));
    set_v_arr(v_arr, 4, c_first, c_middle, c_last, &c_balance);
    bench_lookup(customer, __c.k, 4, col_arr, size_arr, v_arr);

    set_o_k(__o1.k, w_id, d_id, 0);
    set_o_k(__o2.k, w_id, d_id, UINT32_MAX);
    set_col_arr(col_arr, 1, 0);
    arr_size = bench_scan(order, __o1.k, __o2.k, 1, col_arr, k_arr, v_arrs);
    if (arr_size) {
        o_id = ((struct order_k*) k_arr[arr_size - 1])->o_id;
        
        __o1.k.o_id = o_id;
        set_col_arr(col_arr, 2, e_o_entry_d, e_o_carrier_id);
        set_size_arr(size_arr, 2, O_FS(o_entry_d), O_FS(o_carrier_id));
        set_v_arr(v_arr, 2, &o_ent_d, &o_car_id);
        bench_lookup(order, __o1.k, 2, col_arr, size_arr, v_arr);

        set_ol_k(__ol1.k, w_id, d_id, o_id, 0);
        set_ol_k(__ol2.k, w_id, d_id, o_id, UINT32_MAX);

        set_col_arr(col_arr, 5, e_ol_i_id, e_ol_supply_w_id, e_ol_delivery_d, e_ol_quantity, e_ol_amount);
        arr_size = bench_scan(orderline, __ol1.k, __ol2.k, 5, col_arr, k_arr, v_arrs);
        if (arr_size) {
            ol_i_id = *(u32*) v_arrs[0][0];
            ol_supply_w_id = *(u32*) v_arrs[1][0];
            ol_delivery_d = *(u64*) v_arrs[2][0];
            ol_quantity = *(double*) v_arrs[3][0];
            ol_amount = *(double*) v_arrs[4][0];
        }
    }
}

static void __order_status_by_name(u32 w_id, u32 d_id, char* c_last) {
    struct customer_info __ci1, __ci2;
    
    u32 c_id;
    size_t k_arr[100000], v_arrs[1][100000];
    int arr_size;
    int col_arr[20];

    set_c_i_k(__ci1.k, w_id, d_id, c_last, "");
    set_c_i_k(__ci2.k, w_id, d_id, c_last, "~");
    set_col_arr(col_arr, 1, e_ci_c_id);
    arr_size = bench_scan(customer_info, __ci1.k, __ci2.k, 1, col_arr, k_arr, v_arrs);
    if (arr_size) {
        c_id = *(u32*) v_arrs[0][arr_size / 2];
        __order_status_by_c_id(w_id, d_id, c_id);
    }
}

void order_status(u32 w_id) {
    u32 d_id = get_d_id();
    u32 c_id;
    char c_last[16];
    if (get_rand(1, 100) <= USE_C_ID_PR) {
        c_id = get_c_id();
        __order_status_by_c_id(w_id, d_id, c_id);
    } else {
        get_run_name(c_last);
        __order_status_by_name(w_id, d_id, c_last);
    }
}

int work(u32 t_id, u32 w_id) {
#if 0
    int x = get_rand(1, 100);

    if (x <= PY_PR) {
        stm_start();
        payment(t_id, w_id);
        stm_commit();
        return WK_PY;
    }
    
    x -= PY_PR;
    if (x <= DL_PR) {
        stm_start();
        delivery(w_id);
        stm_commit();
        return WK_DL;
    }

    x -= DL_PR;
    if (x <= SL_PR) {
        stm_start();
        stock_level(w_id);
        stm_commit();
        return WK_SL;
    }

    x -= SL_PR;
    if (x <= OS_PR) {
        stm_start();
        order_status(w_id);
        stm_commit();
        return WK_OS;
    }
#endif

    stm_start();
    new_order(w_id);
    stm_commit();

    return WK_NO;
}

void load_warehouse() {
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
        bench_insert(warehouse, __w.k, __w.v, num_w_v, w_size_arr);
    }
}

void load_district(u32 w_id) {
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
        bench_insert(district, __d.k, __d.v, num_d_v, d_size_arr);
    }
}

void load_customer(u32 t_id, u32 w_id, u32 d_id) {
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
        bench_insert(customer, __c.k, __c.v, num_c_v, c_size_arr);
        
        set_c_i_k(__ci.k, w_id, d_id, c_last, c_first);
        set_c_i_v(__ci.v, i);
        bench_insert(customer_info, __ci.k, __ci.v, num_ci_v, ci_size_arr);

        h_pk = atomic_add_return(1, &bench.h_pk[t_id]);
        set_h_k(__h.k, t_id, h_pk);
        get_rand_str(h_data, 12, 24);
        set_h_v(__h.v, i, d_id, w_id, d_id, w_id, get_timestamp(), 10, h_data);
        bench_insert(history, __h.k, __h.v, num_h_v, h_size_arr);
    }
}

void load_order(u32 w_id, u32 d_id) {
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
            bench_insert(neworder, __no.k, __no.v, num_no_v, no_size_arr);
            car_id = 0;
        }
        ol_cnt = get_rand(MIN_O_OL, MAX_O_OL);
        set_o_k(__o.k, w_id, d_id, i);
        set_o_v(__o.v, i, 0, car_id, ol_cnt, 1);
        bench_insert(order, __o.k, __o.v, num_o_v, o_size_arr);

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
            bench_insert(orderline, __ol.k, __ol.v, num_ol_v, ol_size_arr);
        }
    }
}

void load_item(u32 w_id) {
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
        bench_insert(item, __i.k, __i.v, num_i_v, i_size_arr);
    }
}

void load_stock(u32 w_id) {
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
        bench_insert(stock, __s.k, __s.v, num_s_v, s_size_arr);
    }
}

//TODO: correctness verification
