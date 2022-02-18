#ifndef HASH_SET_H
#define HASH_SET_H

#ifdef __cplusplus
extern "C" {
#endif

#include "link_list.h"

#define INIT_ORDER_BUCKETS 	    12  /* a hash_set has INIT_ORDER_BUCKETS at first */
#define LOAD_FACTOR_DEFAULT     4
#define MAIN_ARRAY_LEN 			1024
#define SEGMENT_SIZE 			4096
#define MAX_NUM_BUCKETS 		MAIN_ARRAY_LEN * SEGMENT_SIZE * LOAD_FACTOR_DEFAULT  /* a hash_set can have up to MAX_NUM_BUCKETS buckets */

struct bucket_list {
    struct linked_list bucket_sentinel;   /* head the bucket_list */
};

/* 
 * a continuous memory area contains SEGMENT_SIZE of pointers 
 * (point to bucket_list structure) 
 */
typedef struct bucket_list* segment_t[SEGMENT_SIZE];  

struct hash_set {
    int small;
    int set_size;  /* how many items in the hash_set */
    int load_factor;   /* expect value of the length of each bucket list */
    unsigned long capacity_order; /* how many buckets in the hash_set, capacity_order <= MAX_NUM_BUCKETS */
    segment_t *main_array[MAIN_ARRAY_LEN];  /* main_array is static allocated */
};

#ifdef __cplusplus
}
#endif

#endif
