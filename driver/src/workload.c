#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "types.h"
#include "schema.h"
#include "workload.h"
#include "limit.h"
#include "rand.h"
#include "tpcc.h"
#include "op.h"
#include "atomic.h"

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
    char s_dist[24];
    double ol_amount;

    int i;

    set_w_k(__w.k, w_id);
    tpcc_lookup(warehouse, __w.k, &__w.v);
    w_tax = __w.v.w_tax;
    set_c_k(__c.k, w_id, d_id, c_id);
    tpcc_lookup(customer, __c.k, &__c.v);
    c_discount = __c.v.c_discount;

    spin_lock(district_lock);
    set_d_k(__d.k, w_id, d_id);
    tpcc_lookup(district, __d.k, &__d.v);
    d_tax = __d.v.d_tax;
    __d.v.d_next_o_id++;
    tpcc_update(district, __d.k, __d.v);
    o_id = __d.v.d_next_o_id;
    spin_unlock(district_lock);

    all_local = 1;
    for (i = 0; i < ol_size; i++) {
        if (w_arr[i] != w_id) {
            all_local = 0;
            break;
        }
    }
    set_o_k(__o.k, w_id, d_id, o_id);
    set_o_v(__o.v, c_id, 0, 0, ol_size, all_local);
    tpcc_insert(order, __o.k, __o.v);
    set_no_k(__no.k, w_id, d_id, o_id);
    memset(&__no.v, 0, sizeof(__no.v));
    tpcc_insert(neworder, __no.k, __no.v);
    
    for (i = 0; i < ol_size; i++) {
        set_i_k(__i.k, i_arr[i]);
        tpcc_lookup(item, __i.k, &__i.v);
        i_price = __i.v.i_price;
        set_s_k(__s.k, w_id, i_arr[i]);
        tpcc_lookup(stock, __s.k, &__s.v);
        switch(d_id) {
            case 1:
               strcpy(s_dist, __s.v.s_dist_01);
               break;
            case 2:
               strcpy(s_dist, __s.v.s_dist_02);
               break;
            case 3:
               strcpy(s_dist, __s.v.s_dist_03);
               break;
            case 4:
               strcpy(s_dist, __s.v.s_dist_04);
               break;
            case 5:
               strcpy(s_dist, __s.v.s_dist_05);
               break;
            case 6:
               strcpy(s_dist, __s.v.s_dist_06);
               break;
            case 7:
               strcpy(s_dist, __s.v.s_dist_07);
               break;
            case 8:
               strcpy(s_dist, __s.v.s_dist_08);
               break;
            case 9:
               strcpy(s_dist, __s.v.s_dist_09);
               break;
            case 10:
               strcpy(s_dist, __s.v.s_dist_10);
               break;
        }

        spin_lock(stock_lock);
        set_s_k(__s.k, w_arr[i], i_arr[i]);
        tpcc_lookup(stock, __s.k, &__s.v);
        s_quantity = (s_quantity >= q_arr[i] + 10) ? s_quantity - q_arr[i] : s_quantity + 91 - q_arr[i];
        if (w_arr[i] != w_id) __s.v.s_remote_cnt++;
        __s.v.s_order_cnt++;
        __s.v.s_ytd += q_arr[i];
        tpcc_update(stock, __s.k, __s.v);
        spin_unlock(stock_lock);

        ol_amount = q_arr[i] * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
        set_ol_k(__ol.k, w_id, d_id, o_id, i);
        set_ol_v(__ol.v, i_arr[i], w_arr[i], 0, q_arr[i], ol_amount, s_dist);
        tpcc_insert(orderline, __ol.k, __ol.v);
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

static void __payment_by_c_id(u32 t_id, u32 w_id, u32 d_id, u32 c_w_id, u32 c_d_id, u32 c_id, double h_amount) {
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
    double w_ytd;

    char d_name[10];
    char d_street_1[20];
    char d_street_2[20];
    char d_city[20];
    char d_state[3];
    char d_zip[9];
    double d_ytd;

    char c_data[500];
    char c_credit[3];
    double c_balance;
    double c_ytd_payment;
    double c_payment_cnt;

    double c_new_balance;
    double c_new_ytd_payment;
    double c_new_payment_cnt;
    char   c_new_data[500];

    char h_new_data[24];
    u32 h_pk;

    spin_lock(warehouse_lock);
    set_w_k(__w.k, w_id);
    tpcc_lookup(warehouse, __w.k, &__w.v);
    strcpy(w_name, __w.v.w_name);
    strcpy(w_street_1, __w.v.w_street_1);
    strcpy(w_street_2, __w.v.w_street_2);
    strcpy(w_city, __w.v.w_city);
    strcpy(w_state, __w.v.w_state);
    strcpy(w_zip, __w.v.w_zip);
    w_ytd = __w.v.w_ytd;

    __w.v.w_ytd += h_amount;
    tpcc_update(warehouse, __w.k, __w.v);
    spin_unlock(warehouse_lock);
    
    spin_lock(district_lock);
    set_d_k(__d.k, w_id, d_id);
    tpcc_lookup(district, __d.k, &__d.v);
    strcpy(d_name, __d.v.d_name);
    strcpy(d_street_1, __d.v.d_street_1);
    strcpy(d_street_2, __d.v.d_street_2);
    strcpy(d_city, __d.v.d_city);
    strcpy(d_state, __d.v.d_state);
    strcpy(d_zip, __d.v.d_zip);
    d_ytd = __d.v.d_ytd;

    __d.v.d_ytd += h_amount;
    tpcc_update(district, __d.k, __d.v);
    spin_unlock(district_lock);

    spin_lock(customer_lock);
    set_c_k(__c.k, w_id, c_d_id, c_id);
    tpcc_lookup(customer, __c.k, &__c.v);
    strcpy(c_data, __c.v.c_data);
    strcpy(c_credit, __c.v.c_credit);
    c_balance = __c.v.c_balance;
    c_ytd_payment = __c.v.c_ytd_payment;
    c_payment_cnt = __c.v.c_payment_cnt;

    c_new_balance = c_balance - h_amount;
    c_new_ytd_payment = c_ytd_payment + h_amount;
    c_new_payment_cnt = c_payment_cnt + 1;

    if (strcmp(c_credit, "BC") == 0) {
        snprintf(c_new_data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", 
                c_id, c_d_id, c_w_id, d_id, w_id, h_amount, 0ul, w_name, d_name, c_data);
        strcpy(__c.v.c_data, c_new_data);
    }
    __c.v.c_balance = c_new_balance;
    __c.v.c_ytd_payment = c_new_ytd_payment;
    __c.v.c_payment_cnt = c_new_payment_cnt;
    tpcc_update(customer, __c.k, __c.v);
    spin_unlock(customer_lock);
    
    strcpy(h_new_data, w_name);
    strcat(h_new_data, " ");
    strcat(h_new_data, d_name);
    h_pk = atomic_add_return(1, &tpcc.h_pk[t_id]);
    set_h_k(__h.k, t_id, h_pk);
    set_h_v(__h.v, c_id, c_d_id, c_w_id, d_id, w_id, 0, h_amount, h_new_data);
    tpcc_insert(history, __h.k, __h.v);
}

static void __payment_by_name(u32 t_id, u32 w_id, u32 d_id, u32 c_w_id, u32 c_d_id, char* c_last, double h_amount) {
    struct customer_info __ci1, __ci2;
    
    u32 c_id;
    size_t k_arr[10003], v_arr[10003];
    int arr_size;

    set_c_i_k(__ci1.k, w_id, d_id, c_last, "");
    set_c_i_k(__ci2.k, w_id, d_id, c_last, "~");
    arr_size = tpcc_scan(customer_info, __ci1.k, __ci2.k, k_arr, v_arr);
    c_id = ((struct customer_info_v*) v_arr[arr_size / 2])->c_id;

    __payment_by_c_id(t_id, w_id, d_id, c_w_id, c_d_id, c_id, h_amount);
}

void payment(u32 t_id, u32 w_id) {
    u32 d_id = get_d_id();
    u32 c_w_id;
    u32 c_d_id;
    double h_amount;
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

    if (get_rand(1, 100) <= USE_C_ID_PR) {
        c_id = get_c_id();
        __payment_by_c_id(t_id, w_id, d_id, c_w_id, c_d_id, c_id, h_amount);
    } else {
        get_run_name(c_last);
        // __payment_by_name(t_id, w_id, d_id, c_w_id, c_d_id, c_last, h_amount);
    }
}

static void __delivery(u32 w_id, u32 carrier_id) {
    struct neworder __no1, __no2;
    struct order __o;
    struct orderline __ol;
    struct customer __c;

    size_t k_arr[10009], v_arr[10009];
    int arr_size;
    u32 d_id, o_id;
    u32 ol_cnt, c_id;
    double ol_tot = 0;

    int i;

    for (d_id = 1; d_id <= NUM_D; d_id++) {
        set_no_k(__no1.k, w_id, d_id, 0);
        set_no_k(__no2.k, w_id, d_id, UINT32_MAX);
        arr_size = tpcc_scan(neworder, __no1.k, __no2.k, k_arr, v_arr);
        if (arr_size) {
            o_id = ((struct neworder_k*) k_arr[0])->no_o_id;
            
            spin_lock(order_lock);
            set_o_k(__o.k, w_id, d_id, o_id);
            arr_size = tpcc_scan(order, __o.k, __o.k, k_arr, v_arr);
            if (arr_size) {
                memcpy(&__o.v, (void*)v_arr[0], sizeof(__o.v));
                c_id = __o.v.o_c_id;
                ol_cnt = __o.v.o_ol_cnt;
                __o.v.o_carrier_id = carrier_id;
                tpcc_update(order, __o.k, __o.v);

                set_ol_k(__ol.k, w_id, d_id, o_id, ol_cnt);
                arr_size = tpcc_scan(orderline, __ol.k, __ol.k, k_arr, v_arr);
                if (arr_size) {
                    ol_tot = 0;
                    for (i = 1; i <= ol_cnt; i++) {
                        spin_lock(orderline_lock);
                        set_ol_k(__ol.k, w_id, d_id, o_id, i);
                        tpcc_lookup(orderline, __ol.k, &__ol.v);
                        ol_tot += __ol.v.ol_amount;
                        __ol.v.ol_delivery_d = 0;
                        tpcc_update(orderline, __ol.k, __ol.v);
                        spin_unlock(orderline_lock);
                    }

                    spin_lock(customer_lock);
                    set_c_k(__c.k, w_id, d_id, c_id);
                    tpcc_lookup(customer, __c.k, &__c.v);
                    __c.v.c_balance += ol_tot;
                    __c.v.c_delivery_cnt++;
                    tpcc_update(customer, __c.k, __c.v);
                    spin_unlock(customer_lock);
                }
            } 
            spin_unlock(order_lock);
        }
    }
}

void delivery(u32 w_id) {
    u32 carrier_id = get_carrier();
    __delivery(w_id, carrier_id);
}

static void __stock_level(u32 w_id, u32 d_id, u32 threshold) {
    struct district __d;
    struct stock __s;
    struct orderline __ol1, __ol2;

    u32 o_id;
    size_t k_arr[10009], v_arr[10009];
    u32 i_id;
    int arr_size;
    int i;
    u32 count = 0;

    set_d_k(__d.k, w_id, d_id);
    tpcc_lookup(district, __d.k, &__d.v);
    o_id = __d.v.d_next_o_id;
    
    set_ol_k(__ol1.k, w_id, d_id, o_id - 20, 0);
    set_ol_k(__ol2.k, w_id, d_id, o_id - 1, UINT32_MAX);
    arr_size = tpcc_scan(orderline, __ol1.k, __ol2.k, k_arr, v_arr);
    
    for (i = 0; i < arr_size; i++) {
        i_id = ((struct orderline_v*) v_arr[i])->ol_i_id;
        set_s_k(__s.k, w_id, i_id);
        tpcc_lookup(stock, __s.k, &__s.v);
        if (__s.v.s_quantity < threshold) count++;
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

    size_t k_arr[10009], v_arr[10009];
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

    set_c_k(__c.k, w_id, d_id, c_id);
    tpcc_lookup(customer, __c.k, &__c.v);
    strcpy(c_first, __c.v.c_first);
    strcpy(c_middle, __c.v.c_middle);
    strcpy(c_last, __c.v.c_last);
    c_balance = __c.v.c_balance;

    set_o_k(__o1.k, w_id, d_id, 0);
    set_o_k(__o2.k, w_id, d_id, UINT32_MAX);
    arr_size = tpcc_scan(order, __o1.k, __o2.k, k_arr, v_arr);
    o_id = ((struct order_k*) k_arr[arr_size - 1])->o_id;
    assert(arr_size <= NUM_O);
    
    __o1.k.o_id = o_id;
    tpcc_lookup(order, __o1.k, &__o1.v);
    o_ent_d = __o1.v.o_entry_d;
    o_car_id = __o1.v.o_carrier_id;

    set_ol_k(__ol1.k, w_id, d_id, o_id, 0);
    set_ol_k(__ol2.k, w_id, d_id, o_id, UINT32_MAX);
    arr_size = tpcc_scan(orderline, __ol1.k, __ol2.k, k_arr, v_arr);
    if (arr_size) {
        memcpy(&__ol1.v, (void*) v_arr[0], sizeof(__ol1.v));
        ol_i_id = __ol1.v.ol_i_id;
        ol_supply_w_id = __ol1.v.ol_supply_w_id;
        ol_delivery_d = __ol1.v.ol_delivery_d;
        ol_quantity = __ol1.v.ol_quantity;
        ol_amount = __ol1.v.ol_amount;
    }
}

static void __order_status_by_name(u32 w_id, u32 d_id, char* c_last) {
    struct customer_info __ci1, __ci2;
    
    u32 c_id;
    size_t k_arr[10003], v_arr[10003];
    int arr_size;

    set_c_i_k(__ci1.k, w_id, d_id, c_last, "");
    set_c_i_k(__ci2.k, w_id, d_id, c_last, "~");
    arr_size = tpcc_scan(customer_info, __ci1.k, __ci2.k, k_arr, v_arr);
    c_id = ((struct customer_info_v*) v_arr[arr_size / 2])->c_id;

    __order_status_by_c_id(w_id, d_id, c_id);
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
        // __order_status_by_name(w_id, d_id, c_last);
    }
}

int work(u32 t_id, u32 w_id) {
    int x = get_rand(1, 100);

    if (x <= PY_PR) {
        payment(t_id, w_id);
        return WK_PY;
    }
    
    x -= PY_PR;
    if (x <= DL_PR) {
        delivery(w_id);
        return WK_DL;
    }

    x -= DL_PR;
    if (x <= SL_PR) {
        stock_level(w_id);
        return WK_SL;
    }

    x -= SL_PR;
    if (x <= OS_PR) {
        order_status(w_id);
        return WK_OS;
    }

    new_order(w_id);
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
        tpcc_insert(warehouse, __w.k, __w.v);
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
        tpcc_insert(district, __d.k, __d.v);
    }
}

void load_customer(u32 t_id, u32 w_id, u32 d_id) {
    int i;
    struct customer __c;
    struct history __h;

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
        tpcc_insert(customer, __c.k, __c.v);
        
        h_pk = atomic_add_return(1, &tpcc.h_pk[t_id]);
        set_h_k(__h.k, t_id, h_pk);
        get_rand_str(h_data, 12, 24);
        set_h_v(__h.v, i, d_id, w_id, d_id, w_id, 0, 10, h_data);
        tpcc_insert(history, __h.k, __h.v);
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
            tpcc_insert(neworder, __no.k, __no.v);
            car_id = 0;
        }
        ol_cnt = get_rand(MIN_O_OL, MAX_O_OL);
        set_o_k(__o.k, w_id, d_id, i);
        set_o_v(__o.v, i, 0, car_id, ol_cnt, 1);
        tpcc_insert(order, __o.k, __o.v);

        for (j = 1; j <= ol_cnt; j++) {
            if (i < 2101) {
                ol_amount = 0;
            } else {
                ol_amount = get_rand_lf(0.01, 9999.99);
            }
            i_id = get_rand(1, NUM_I);
            set_ol_k(__ol.k, w_id, d_id, i, j);
            get_rand_str(dist_info, 23, 24);
            set_ol_v(__ol.v, get_rand(1, NUM_I), w_id, 0, 5, ol_amount, dist_info);
            tpcc_insert(orderline, __ol.k, __ol.v);
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
        tpcc_insert(item, __i.k, __i.v);
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
        tpcc_insert(stock, __s.k, __s.v);
    }
}

// TODO: define TIMESTAMP(datetime) & delivery_d
// FIXME: 10001 -> NUM_C; 10009->NUM_O
// TODO: lookup when find none