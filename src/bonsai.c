/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */

#include "bonsai.h"

buffer_node_t* buffer_layer_init() {
    buffer_node_t* b_layer = (buffer_node_t*) malloc(sizeof(buffer_layer_t));
    assert(b_layer);
    int i;
    for (i = 0; i < CPU_MAX_NUM; i++) {
        b_layer->log_list[i].head = (op_log_t*) malloc(sizeof(op_log_t));
        assert(b_layer->log_list[i].head);
        b_layer->log_list[i].tail = b_layer->log_list[i].head;
        b_layer->log_list[i].head->next = NULL;
    }
    return b_layer;
}

bonsai_t* get_bosai() {
    PMEMoid root = pmemobj_root(pop[0], sizeof(bonsai_t));
    return pmemobj_direct(root);
}

persist_node_t* alloc_persist_node() {
    TOID(persist_node_t) toid;
    POBJ_ZALLOC(pop[0], &toid, persist_node_t, sizeof(persist_node_t));
    D_RW(toid)->slot_lock = rwlock_init();
    D_RW(toid)->buffer_node = cuckoo_init(BUFFER_SIZE_MUL * NODE_SIZE);
    return pmemobj_direct(toid);
}

persist_layer_t* persist_layer_init() {
    p_layer->tail = alloc_persist_node();
    p_layer->tail
    
    p_layer->head->bitmap = 1;
}

bonsai_t* bonsai_init() {
    bonsai_t* bonsai = get_bonsai();
    if (!bonsai->init) {
        INDEX(bonsai) = sl_init();
        BUFFER(bonsai) = buffer_layer_init();
        PERSIST(bonsai) = persist_layer_init();
    } else {
        bonsai_recover(bonsai);
    }
    return bonsai;
}