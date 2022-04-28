#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdarg.h>

#include "types.h"

#define field_size(structure, field)    (sizeof(((structure*)0)->field))
#define field_offset(structure, field)  ((size_t)&((structure*)0)->field)

#define W_FS(field) 	field_size(struct warehouse_v, field)
#define D_FS(field) 	field_size(struct district_v, field)
#define C_FS(field) 	field_size(struct customer_v, field)
#define H_FS(field) 	field_size(struct history_v, field)
#define NO_FS(field) 	field_size(struct neworder_v, field)
#define O_FS(field) 	field_size(struct order_v, field)
#define OL_FS(field) 	field_size(struct orderline_v, field)
#define I_FS(field) 	field_size(struct item_v, field)
#define S_FS(field) 	field_size(struct stock_v, field)
#define CI_FS(field) 	field_size(struct customer_info_v, field)

#define W_FO(field) 	field_offset(struct warehouse_v, field)
#define D_FO(field) 	field_offset(struct district_v, field)
#define C_FO(field) 	field_offset(struct customer_v, field)
#define H_FO(field) 	field_offset(struct history_v, field)
#define NO_FO(field) 	field_offset(struct neworder_v, field)
#define O_FO(field) 	field_offset(struct order_v, field)
#define OL_FO(field) 	field_offset(struct orderline_v, field)
#define I_FO(field) 	field_offset(struct item_v, field)
#define S_FO(field) 	field_offset(struct stock_v, field)
#define CI_FO(field) 	field_offset(struct customer_info_v, field)

enum {
	E_W = 1,
	E_D,
	E_C,
	E_H,
	E_NO,
	E_O,
	E_OL,
	E_I,
	E_S,
	E_CI
};

struct warehouse_k {
	int 	id;
	int 	col;
	u32     w_id;
}__attribute__((packed));
struct warehouse_v {
	char    w_name[10];
	char    w_street_1[20];
	char    w_street_2[20];
	char    w_city[20];
	char    w_state[3];
	char    w_zip[9];
	double  w_tax;
	double  w_ytd;
}__attribute__((packed));
struct warehouse {
	struct warehouse_k k;
	struct warehouse_v v;
}__attribute__((packed));
enum {
	e_w_name = 0,
	e_w_street_1,
	e_w_street_2,
	e_w_city,
	e_w_state,
	e_w_zip,
	e_w_tax,
	e_w_ytd
};

struct district_k {
	int 	id;
	int 	col;
	u32 	d_w_id;
	u32 	d_id;
}__attribute__((packed));
struct district_v {
	char 	d_name[10];
	char 	d_street_1[20];
	char 	d_street_2[20];
	char 	d_city[20];
	char 	d_state[3];
	char 	d_zip[9];
	double 	d_tax;
	double 	d_ytd;
	u32 	d_next_o_id;
}__attribute__((packed));
struct district {
	struct district_k k;
	struct district_v v;
}__attribute__((packed));
enum {
	e_d_name = 0,
	e_d_street_1,
	e_d_street_2,
	e_d_city,
	e_d_state,
	e_d_zip,
	e_d_tax,
	e_d_ytd,
	e_d_next_o_id
};

struct customer_k {
	int 	id;
	int 	col;
	u32 	c_w_id;
	u32 	c_d_id;
	u32 	c_id;
}__attribute__((packed));
struct customer_v {
	char 	c_first[16];
	char 	c_middle[3];
	char 	c_last[16];
	char 	c_street_1[20];
	char 	c_street_2[20];
	char 	c_city[20];
	char 	c_state[3];
	char 	c_zip[9];
	char 	c_phone[16];
	u64 	c_since;
	char 	c_credit[3];
	double 	c_credit_lim;
	double 	c_discount;
	double 	c_balance;
	double 	c_ytd_payment;
	double 	c_payment_cnt;
	double 	c_delivery_cnt;
	char 	c_data[500];
}__attribute__((packed));
struct customer {
	struct customer_k k;
	struct customer_v v;
}__attribute__((packed));
enum {
	e_c_first = 0,
	e_c_middle,
	e_c_last,
	e_c_street_1,
	e_c_street_2,
	e_c_city,
	e_c_state,
	e_c_zip,
	e_c_phone,
	e_c_since,
	e_c_credit,
	e_c_credit_lim,
	e_c_discount,
	e_c_balance,
	e_c_ytd_payment,
	e_c_payment_cnt,
	e_c_delivery_cnt,
	e_c_data
};

struct history_k {
	int 	id;
	int 	col;
	u32 	thread_id;
	u32 	h_pk;
}__attribute__((packed));
struct history_v {
	u32 	h_c_id;
	u32 	h_c_d_id;
	u32 	h_c_w_id;
	u32 	h_d_id;
	u32 	h_w_id;
	u64 	h_date;
	double 	h_amount;
	char 	h_data[24];
}__attribute__((packed));
struct history {
	struct history_k k;
	struct history_v v;
}__attribute__((packed));
enum {
	e_h_c_id = 0,
	e_h_c_d_id,
	e_h_c_w_id,
	e_h_d_id,
	e_h_w_id,
	e_h_date,
	e_h_amount,
	e_h_data
};

struct neworder_k {
	int id;
	int 	col;
	u32 	no_w_id;
	u32 	no_d_id;
	u32 	no_o_id;
}__attribute__((packed));
struct neworder_v {
}__attribute__((packed));
struct neworder {
	struct neworder_k k;
	struct neworder_v v;
}__attribute__((packed));

struct order_k {
	int 	id;
	int 	col;
	u32 	o_w_id;
	u32 	o_d_id;
	u32 	o_id;
}__attribute__((packed));
struct order_v {
	u32 	o_c_id;
	u64 	o_entry_d;
	u32 	o_carrier_id;
	double 	o_ol_cnt;
	double 	o_all_local;
}__attribute__((packed));
struct order {
	struct order_k k;
	struct order_v v;
}__attribute__((packed));
enum {
	e_o_c_id = 0,
	e_o_entry_d,
	e_o_carrier_id,
	e_o_ol_cnt,
	e_o_all_local
};

struct orderline_k {
	int 	id;
	int 	col;
	u32 	ol_w_id;
	u32 	ol_d_id;
	u32 	ol_o_id;
	u32 	ol_number;
}__attribute__((packed));
struct orderline_v {
	u32 	ol_i_id;
	u32 	ol_supply_w_id;
	u64 	ol_delivery_d;
	double 	ol_quantity;
	double 	ol_amount;
	char 	ol_dist_info[24];
}__attribute__((packed));
struct orderline {
	struct orderline_k k;
	struct orderline_v v;
}__attribute__((packed));
enum {
	e_ol_i_id = 0,
	e_ol_supply_w_id,
	e_ol_delivery_d,
	e_ol_quantity,
	e_ol_amount,
	e_ol_dist_info
};

struct item_k {
	int 	id;
	int 	col;
	u32 	i_id;
}__attribute__((packed));
struct item_v {
	u32 	i_im_id;
	char 	i_name[24];
	double 	i_price;
	char 	i_data[50];
}__attribute__((packed));
struct item {
	struct item_k k;
	struct item_v v;
}__attribute__((packed));
enum {
	e_i_im_id = 0,
	e_i_name,
	e_i_price,
	e_i_data,
};

struct stock_k {
	int		id;
	int 	col;
	u32 	s_w_id;
	u32 	s_i_id;
}__attribute__((packed));
struct stock_v {
	double 	s_quantity;
	char 	s_dist_01[24];
	char 	s_dist_02[24];
	char 	s_dist_03[24];
	char 	s_dist_04[24];
	char 	s_dist_05[24];
	char 	s_dist_06[24];
	char 	s_dist_07[24];
	char 	s_dist_08[24];
	char 	s_dist_09[24];
	char 	s_dist_10[24];
	double 	s_ytd;
	double 	s_order_cnt;
	double 	s_remote_cnt;
	char 	s_data[50];
}__attribute__((packed));
struct stock {
	struct stock_k k;
	struct stock_v v;
}__attribute__((packed));
enum {
	e_s_quantity = 0,
	e_s_dist_01,
	e_s_dist_02,
	e_s_dist_03,
	e_s_dist_04,
	e_s_dist_05,
	e_s_dist_06,
	e_s_dist_07,
	e_s_dist_08,
	e_s_dist_09,
	e_s_dist_10,
	e_s_ytd,
	e_s_order_cnt,
	e_s_remote_cnt,
	e_s_data
};

struct customer_info_k {
	int 	id;
	int 	col;
	u32 	c_w_id;
	u32 	c_d_id;
	char 	c_last[16];
	char 	c_first[16];
}__attribute__((packed));
struct customer_info_v {
	u32 	c_id;
}__attribute__((packed));
struct customer_info {
	struct customer_info_k k;
	struct customer_info_v v;
}__attribute__((packed));
enum {
	e_ci_c_id = 0
};

#define num_w_v 	8
#define num_d_v 	9
#define num_c_v 	18
#define num_h_v 	8
#define num_no_v 	0
#define num_o_v 	5
#define num_ol_v	6
#define num_i_v 	4
#define num_s_v 	15
#define num_ci_v 	1

static const int w_size_arr[num_w_v] = {10, 20, 20, 20, 3, 9, 8, 8};
static const int d_size_arr[num_d_v] = {10, 20, 20, 20, 3, 9, 8, 8, 4};
static const int c_size_arr[num_c_v] = {16, 3, 16, 20, 20, 20, 3, 9, 16, 8, 3, 8, 8, 8, 8, 8, 8, 500};
static const int h_size_arr[num_h_v] = {4, 4, 4, 4, 4, 8, 8, 24};
static const int *no_size_arr = NULL;
static const int o_size_arr[num_o_v] = {4, 8, 4, 8, 8};
static const int ol_size_arr[num_ol_v] = {4, 4, 8, 8, 8, 24};
static const int i_size_arr[num_i_v] = {4, 24, 8, 50};
static const int s_size_arr[num_s_v] = {8, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 8, 8, 8, 50};
static const int ci_size_arr[num_ci_v] = {4};

static inline void set_int_arr(int* arr, int num, ...) {
	int i;
	va_list valist;

	va_start(valist, num);
	for (i = 0; i < num; i++) {
		arr[i] = va_arg(valist, int);
	}
	va_end(valist);
}

#define set_off_arr(off_arr, num, args...) set_int_arr(off_arr, num, args)
#define set_size_arr(size_arr, num, args...) set_int_arr(size_arr, num, args)

static inline void set_col_arr(int* col_arr, int num, ...) {
	int i;
	va_list valist;

	va_start(valist, num);
	for (i = 0; i < num; i++) {
		col_arr[i] = va_arg(valist, int);
	}
	va_end(valist);
}

static inline void set_col_arr2(int* col_arr, int num) {
	int i;
	for(i = 0; i < num; i++) {
		col_arr[i] = i;
	}
}

static inline void set_v_arr(void** v_arr, int num, ...) {
	int i;
	va_list valist;

	va_start(valist, num);
	for (i = 0; i < num; i++) {
		v_arr[i] = va_arg(valist, void*);
	}
	va_end(valist);
}


#define set_w_k(w, _w_id) 							w.id = E_W; w.w_id = _w_id; 
#define set_d_k(d, _w_id, _d_id) 					d.id = E_D; d.d_w_id = _w_id; d.d_id = _d_id
#define set_c_k(c, _w_id, _d_id, _c_id) 			c.id = E_C; c.c_w_id = _w_id; c.c_d_id = _d_id; c.c_id = _c_id
#define set_h_k(h, _t_id, _h_pk)					h.id = E_H; h.thread_id = _t_id; h.h_pk = _h_pk
#define set_no_k(no, _w_id, _d_id, _o_id) 			no.id = E_NO; no.no_w_id = _w_id; no.no_d_id = _d_id; no.no_o_id = _o_id
#define set_o_k(o, _w_id, _d_id, _o_id)				o.id = E_O; o.o_w_id = _w_id; o.o_d_id = _d_id; o.o_id = _o_id
#define set_ol_k(ol, _w_id, _d_id, _o_id, _n)		ol.id = E_OL; ol.ol_w_id = _w_id; ol.ol_d_id = _d_id; ol.ol_o_id = _o_id; ol.ol_number = _n
#define set_i_k(i, _i_id)							i.id = E_I; i.i_id = _i_id
#define set_s_k(s, _w_id, _i_id)					s.id = E_S; s.s_w_id = _w_id; s.s_i_id = _i_id
#define set_c_i_k(ci, _w_id, _d_id, _c_l, _c_f)     ci.id = E_CI; ci.c_w_id = _w_id; ci.c_d_id = _d_id; \
													strcpy(ci.c_last, _c_l); strcpy(ci.c_first, _c_f)
#define set_w_v(w, _name, _s1, _s2, _ct, _st, _zp, _tx, _ytd) \
		strcpy(w.w_name, _name);	\
		strcpy(w.w_street_1, _s1);	\
		strcpy(w.w_street_2, _s2);	\
		strcpy(w.w_city, _ct);	\
		strcpy(w.w_state, _st);	\
		strcpy(w.w_zip, _zp);	\
		w.w_tax = _tx; \
		w.w_ytd = _ytd
#define set_d_v(d, _name, _s1, _s2, _ct, _st, _zp, _tx, _ytd, _o_id) \
		strcpy(d.d_name, _name);	\
		strcpy(d.d_street_1, _s1);	\
		strcpy(d.d_street_2, _s2);	\
		strcpy(d.d_city, _ct);	\
		strcpy(d.d_state, _st);	\
		strcpy(d.d_zip, _zp);	\
		d.d_tax = _tx; \
		d.d_ytd = _ytd; \
		d.d_next_o_id = _o_id
#define set_c_v(c, _fr, _md, _ls, _s1, _s2, _ct, _st, _zp, _ph, _sc, _cr, _crl, _ds, _bl, _ytdp, _pc, _dc, _data) \
		strcpy(c.c_first, _fr);	\
		strcpy(c.c_middle, _md);	\
		strcpy(c.c_last, _ls);	\
		strcpy(c.c_street_1, _s1);	\
		strcpy(c.c_street_2, _s2);	\
		strcpy(c.c_city, _ct);	\
		strcpy(c.c_state, _st);	\
		strcpy(c.c_zip, _zp);	\
		strcpy(c.c_phone, _ph);	\
		c.c_since = _sc; \
		strcpy(c.c_credit, _cr);	\
		c.c_credit_lim = _crl; \
		c.c_discount = _ds; \
		c.c_balance = _bl; \
		c.c_ytd_payment = _ytdp; \
		c.c_payment_cnt = _pc; \
		c.c_delivery_cnt = _dc; \
		strcpy(c.c_data, _data)
#define set_h_v(h, _c_id, _c_d_id, _c_w_id, _d_id, _w_id, _date, _amt, _data) \
		h.h_c_id = _c_id; \
		h.h_c_d_id = _c_d_id; \
		h.h_c_w_id = _c_w_id; \
		h.h_d_id = _d_id; \
		h.h_w_id = _w_id; \
		h.h_date = _date; \
		h.h_amount = _amt; \
		strcpy(h.h_data, _data)
#define set_no_v(no) while(0)
#define set_o_v(o, _c_id, _ent_d, _car_id, _ol_cnt, _all_lcl) \
		o.o_c_id = _c_id; \
		o.o_entry_d = _ent_d; \
		o.o_carrier_id = _car_id; \
		o.o_ol_cnt = _ol_cnt; \
		o.o_all_local = _all_lcl
#define set_ol_v(ol, _i_id, _w_id, _d_d, _qty, _amt, _dist_info) \
		ol.ol_i_id = _i_id; \
		ol.ol_supply_w_id = _w_id; \
		ol.ol_delivery_d = _d_d; \
		ol.ol_quantity = _qty; \
		ol.ol_amount = _amt; \
		strcpy(ol.ol_dist_info, _dist_info)
#define set_i_v(i, _im_id, _name, _pr, _data) \
		i.i_im_id = _im_id; \
		strcpy(i.i_name, _name); \
		i.i_price = _pr; \
		strcpy(i.i_data, _data)
#define set_s_v(s, _qty, _d_arr, _ytd, _odc, _rmtc, _data) \
		s.s_quantity = _qty; \
		strcpy(s.s_dist_01, _d_arr[0]); \
		strcpy(s.s_dist_02, _d_arr[1]); \
		strcpy(s.s_dist_03, _d_arr[2]); \
		strcpy(s.s_dist_04, _d_arr[3]); \
		strcpy(s.s_dist_05, _d_arr[4]); \
		strcpy(s.s_dist_06, _d_arr[5]); \
		strcpy(s.s_dist_07, _d_arr[6]); \
		strcpy(s.s_dist_08, _d_arr[7]); \
		strcpy(s.s_dist_09, _d_arr[8]); \
		strcpy(s.s_dist_10, _d_arr[9]); \
		s.s_ytd = _ytd; \
		s.s_order_cnt = _odc; \
		s.s_remote_cnt = _rmtc; \
		strcpy(s.s_data, _data)
#define set_c_i_v(ci, _c_id) \
		ci.c_id = _c_id

#endif