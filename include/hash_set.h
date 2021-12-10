#ifndef HASH_SET_H
#define HASH_SET_H

#ifdef __cplusplus
extern "C" {
#endif

#include "link_list.h"

#define MAX_NUM_BUCKETS 		64  /* a hash_set can have up to MAX_NUM_BUCKETS buckets */
#define INIT_NUM_BUCKETS 		2  /* a hash_set has INIT_NUM_BUCKETS at first */
#define LOAD_FACTOR_DEFAULT 	0.75
#define MAIN_ARRAY_LEN 			16
#define SEGMENT_SIZE 			4

struct bucket_list {
    struct linked_list bucket_sentinel;   /* head the bucket_list */
};

/* 
 * a continuous memory area contains SEGMENT_SIZE of pointers 
 * (point to bucket_list structure) 
 */
typedef struct bucket_list* segment_t[SEGMENT_SIZE];  

struct hash_set {
    segment_t* main_array[MAIN_ARRAY_LEN];  /* main_array is static allocated */
    float load_factor;   /* expect value of the length of each bucket list */
	int set_size;  /* how many items in the hash_set */
    unsigned long capacity; /* how many buckets in the hash_set, capacity <= MAX_NUM_BUCKETS */
};

extern void hs_init(struct hash_set * hs);
extern int hs_insert(struct hash_set* hs, int tid, pkey_t key, pval_t* addr);
extern pval_t* hs_lookup(struct hash_set* hs, int tid, pkey_t key);
extern int hs_remove(struct hash_set* hs, int tid, pkey_t key); 
extern void hs_destroy(struct hash_set* hs);

#ifdef __cplusplus
}
#endif

#endif
