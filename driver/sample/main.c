#include <stdio.h>

#include "skiplist.c"
#include "tpcc.h"

int main() {
    tpcc_init();
    tpcc_load();
    tpcc_init();
}