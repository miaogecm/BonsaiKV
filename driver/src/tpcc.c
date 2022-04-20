#include "rand.h"
#include "tpcc.h"

struct tpcc tpcc;
extern void tpcc_init(init_func_t init, destory_func_t destory,
				      insert_func_t insert, update_func_t update, remove_func_t remove,
				      lookup_func_t lookup, scan_func_t scan) {
    int i;

    tpcc.init = init;
    tpcc.destroy = destory;
    tpcc.insert = insert;
    tpcc.update = update;
    tpcc.remove = remove;
    tpcc.lookup = lookup;
    tpcc.scan = scan;

    for (i = 0; i < MAX_CPU; i++) {
        atomic_set(tpcc.h_pk[i], 0);
    }

    rand_init();
}

void tpcc_load() {
    load_item();
    load_
}

void tpcc_start() {

}
