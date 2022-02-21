#ifndef HASH_SET_H
#define HASH_SET_H

#ifdef __cplusplus
extern "C" {
#endif

#include "link_list.h"
#include "atomic128.h"
#include "rwlock.h"

#define INIT_ORDER_BUCKETS 	    1  /* a hash_set has INIT_ORDER_BUCKETS at first */
#define LOAD_FACTOR_DEFAULT     6
#define MAIN_ARRAY_LEN 			1024
#define SEGMENT_SIZE 			4096
#define MAX_NUM_BUCKETS 		MAIN_ARRAY_LEN * SEGMENT_SIZE * LOAD_FACTOR_DEFAULT  /* a hash_set can have up to MAX_NUM_BUCKETS buckets */
#define SMALL_HASHSET_MAXN      32
#define SMALL_HASHSET_SLOT_SFT  9
#define SMALL_HASHSET_SLOTN     (1 << SMALL_HASHSET_SLOT_SFT)

struct bucket_list {
    struct linked_list bucket_sentinel;   /* head the bucket_list */
};

typedef struct {
    pkey_t k;
    pval_t *v;
} kvpair_t;

/* 
 * a continuous memory area contains SEGMENT_SIZE of pointers 
 * (point to bucket_list structure) 
 */
typedef struct bucket_list* segment_t[SEGMENT_SIZE];  

struct hash_set {
    rwlock_t transform_lock;
    int small;
    union atomic_u128 avg;
    int set_size;  /* how many items in the hash_set */
    union {
        struct {
            int load_factor;   /* expect value of the length of each bucket list */
            unsigned long capacity_order; /* how many buckets in the hash_set, capacity_order <= MAX_NUM_BUCKETS */
            segment_t *main_array[MAIN_ARRAY_LEN];  /* main_array is static allocated */
        };
        struct {
            kvpair_t slots[SMALL_HASHSET_SLOTN];
        };
    };
};

extern void hs_init(struct hash_set * hs);
extern int hs_insert(struct hash_set* hs, int tid, pkey_t key, pval_t* addr);
extern int hs_update(struct hash_set* hs, int tid, pkey_t key, pval_t* val);
extern pval_t* hs_lookup(struct hash_set* hs, int tid, pkey_t key);
extern int hs_remove(struct hash_set* hs, int tid, pkey_t key); 
extern void hs_destroy(struct hash_set* hs);

struct pnode;
extern void hs_scan_and_split(struct hash_set *old, struct hash_set *new, struct hash_set *mid, pkey_t max, pkey_t avg_key, struct pnode* new_pnode, struct pnode* mid_pnode);

typedef void (*hs_exec_t)(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);

extern void hs_scan_and_ops(struct hash_set* hs, hs_exec_t exec, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);
extern void hs_search_key(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);
extern void hs_print_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);
extern void hs_check_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);
extern void hs_count_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5);

#ifdef __cplusplus
}
#endif

#endif
