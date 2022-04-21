#ifndef TRADE_H
#define TRADE_H

#include "types.h"

enum {
    WK_NO = 1,
    WK_PY,
    WK_DL,
    WK_SL,
    WK_OS
};

extern void new_order(u32 w_id);
extern void payment(u32 t_id, u32 w_id);
extern void delivery(u32 w_id);
extern void stock_level(u32 w_id);
extern void order_status(u32 w_id);
extern int work(u32 t_id, u32 w_id);

extern void load_warehouse();
extern void load_district(u32 w_id);
extern void load_customer(u32 t_id, u32 w_id, u32 d_id);
extern void load_order(u32 w_id, u32 d_id);
extern void load_item();
extern void load_stock(u32 w_id);

#endif