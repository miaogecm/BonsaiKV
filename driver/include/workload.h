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

extern void tpcc_new_order(u32 w_id);
extern void tpcc_payment(u32 t_id, u32 w_id);
extern void tpcc_delivery(u32 w_id);
extern void tpcc_stock_level(u32 w_id);
extern void tpcc_order_status(u32 w_id);

extern void tpcc_load_warehouse();
extern void tpcc_load_district(u32 w_id);
extern void tpcc_load_customer(u32 t_id, u32 w_id, u32 d_id);
extern void tpcc_load_order(u32 w_id, u32 d_id);
extern void tpcc_load_item();
extern void tpcc_load_stock(u32 w_id);

extern void tatp_loadSubscriber(int start, int end);
extern void tatp_loadAccessInfo(int start, int end);
extern void tatp_loadSpecialFacility_CallForwarding(int start, int end);

extern int tpcc_work(u32 t_id, u32 w_id);
extern int tatp_work(u32 t_id);

#endif