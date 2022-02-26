/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *
 * A lock-free extendible hash table implementation. 
 * Split-Ordered Lists: Lock-Free Extensible Hash Tables. Ori Shalev, Nir Shavit. Journal of the ACM, Vol.53, No.3, 2006, pp.379�405
 *
 * A lock-free hash table consists of two components: 
 * 
 * (1) a lock-free linked list
 * (2) an expanding array of references into the list
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <thread.h>
#include <assert.h>
#include <limits.h>

#include "hash.h"
#include "hash_set.h"
#include "common.h"
#include "atomic.h"
#include "pnode.h"
#include "oplog.h"
#include "mptable.h"
#include "bonsai.h"

#define HI_MASK 0x8000000000000000  /* the MSB of a unsigned long value */

#ifdef BONSAI_HASHSET_DEBUG
static int hs_node_count = 0;
static int hs_add_success_time = 0;
static int hs_add_fail_time = 0;
static int hs_remove_success_time = 0;
static int hs_remove_fail_time = 0;
#endif

#define MASK(x)     ((1ul << (x)) - 1)

static void bucket_list_init(struct bucket_list** bucket, int bucket_index);
static struct bucket_list* get_bucket_list(struct hash_set* hs, int bucket_index);
static void set_bucket_list(struct hash_set* hs, int tid, int bucket_index, struct bucket_list* new_bucket);
static void initialize_bucket(struct hash_set* hs, int tid, int bucket_index);
static int get_parent_index(struct hash_set* hs, int bucket_index);

static const unsigned char byte_rev_lut[256] = {
    0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
    0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
    0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
    0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
    0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
    0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
    0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
    0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
    0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
    0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
    0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
    0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
    0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
    0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
    0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
    0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF
};

static inline pkey_t reverse(pkey_t code) {
    unsigned long val;
    unsigned char *src = (unsigned char *) &code, *dst = (unsigned char *) &val;
    dst[7] = byte_rev_lut[src[0]];
    dst[6] = byte_rev_lut[src[1]];
    dst[5] = byte_rev_lut[src[2]];
    dst[4] = byte_rev_lut[src[3]];
    dst[3] = byte_rev_lut[src[4]];
    dst[2] = byte_rev_lut[src[5]];
    dst[1] = byte_rev_lut[src[6]];
    dst[0] = byte_rev_lut[src[7]];
    return val;
}

/*
 * make_ordinary_key: set MSB to 1, and reverse the key.
 */
static pkey_t make_ordinary_key(pkey_t key) {
    return reverse(key);  //mark the MSB and reverse it.
}

/* 
 * make_sentinel_key: set MSB to 0, and reverse the key.
 */
static pkey_t make_sentinel_key(pkey_t key) {
    return reverse(key);
}

#if 0
/*
 * is_sentinel_key: 1 if key is sentinel key, 0 if key is ordinary key.
 */
static int is_sentinel_key(pkey_t key) {
    if (key == ((key >> 1) << 1)) {
        return 1;
    }
    return 0;
}
#endif

static int is_sentinel_node(struct ll_node* ll_node) {
    return ll_node->is_sentinel_key;
}

static pkey_t get_origin_key(pkey_t key) {
    return reverse(key);
}

/* 
 * get_bucket_list: if the segment of the target bucket_index hasn't been initialized, 
 * initialize the segment.
 */
static struct bucket_list* get_bucket_list(struct hash_set* hs, int bucket_index) {
    int main_array_index = bucket_index / SEGMENT_SIZE;
    int segment_index = bucket_index % SEGMENT_SIZE;
	struct bucket_list** buckets;
	segment_t* old_value;
	
    // First, find the segment in the hs->main_array.
    segment_t* p_segment = hs->main_array[main_array_index];
    if (p_segment == NULL) {
        //allocate the segment until the first bucket_list in it is visited.
        p_segment = (segment_t*) malloc(sizeof(segment_t));
        memset(p_segment, 0, sizeof(segment_t));  //important

		old_value = cmpxchg(&hs->main_array[main_array_index], NULL, p_segment);
        if (old_value != NULL) {  //someone beat us to it.
            free(p_segment);
            p_segment = hs->main_array[main_array_index];
        } else {
            //bonsai_debug("allocate a new segment [%d] ==> %016lx\n", main_array_index, p_segment);
        }
    }
    
    // Second, find the bucket_list in the segment.
    buckets = (struct bucket_list**)p_segment;
    return buckets[segment_index];  //may be NULL.
}

/* 
 * bucket_list_init: init a bucket_list structure. It contains a sentinel node.
 */
static void bucket_list_init(struct bucket_list** bucket, int bucket_index) {		
    *bucket = (struct bucket_list*) malloc(sizeof(struct bucket_list));  //don't need memset(,0,);
    
    ll_init(&((*bucket)->bucket_sentinel));  // init the linked_list of the bucket-0.
    
    (*bucket)->bucket_sentinel.ll_head.key = make_sentinel_key(bucket_index);
    (*bucket)->bucket_sentinel.ll_head.is_sentinel_key = 1;
}

#define EMPTY_SLOT_KEY      (ULONG_MAX - 1)

void __hs_init_small(struct hash_set *hs) {
    int i;
    for (i = 0; i < SMALL_HASHSET_SLOTN; i++) {
        hs->slots[i] = (kvpair_t) { EMPTY_SLOT_KEY, NULL };
    }
}

void hs_init(struct hash_set* hs) {
    struct bucket_list **segment;

    hs->set_size = 0;
    hs->avg.val = 0;

    if (hs->small) {
        __hs_init_small(hs);
        return;
    }

    memset(hs->main_array, 0, MAIN_ARRAY_LEN * sizeof(segment_t *));  //important.
    hs->load_factor = LOAD_FACTOR_DEFAULT;  // 2
    hs->capacity_order = INIT_ORDER_BUCKETS;  // 2 at first

    get_bucket_list(hs, 0);  //allocate the memory of the very first segment
    segment = (struct bucket_list **) hs->main_array[0];

    bucket_list_init(&segment[0], 0);   //here we go.
}

/* 
 * set_bucket_list: put the new_bucket at the bucket_index.
 */
static void set_bucket_list(struct hash_set* hs, int tid, int bucket_index, struct bucket_list* new_bucket){
    int main_array_index = bucket_index / SEGMENT_SIZE;
    int segment_index = bucket_index % SEGMENT_SIZE;
	struct bucket_list** buckets;
	segment_t* old_value;

    // First, find the segment in the hs->main_array.
    segment_t* p_segment = hs->main_array[main_array_index];
    if (p_segment == NULL) {
        //allocate the segment until the first bucket_list in it is visited.
        p_segment = (segment_t*) malloc(sizeof(segment_t));
        memset(p_segment, 0, sizeof(segment_t));  //important

		old_value = cmpxchg(&hs->main_array[main_array_index], NULL, p_segment);
        if (old_value != NULL) {  //someone beat us to it.
            free(p_segment);
            p_segment = hs->main_array[main_array_index];
        } else {
        	;
            //bonsai_debug("allocate a new segment [%d] ==> %016lx\n", main_array_index, p_segment);
        }
    }
    
    // Second, find the bucket_list in the segment.
    buckets = (struct bucket_list**)p_segment;
    buckets[segment_index] = new_bucket;
}


/*
 * bucket_list_lookup: return 1 if contains, 0 otherwise.
 */
pval_t* bucket_list_lookup(struct bucket_list* bucket, int tid, pkey_t key) {
   struct ll_node* node;
   node = ll_lookup(&bucket->bucket_sentinel, tid, key);
   if (node == NULL) {
       return NULL;
   }
   return node->val;
}

/*
 * get the parent bucket index of the given bucket_index
 */
static int get_parent_index(struct hash_set* hs, int bucket_index) {
    int parent_index = 1 << hs->capacity_order;

    do {
        parent_index = parent_index >> 1;
    } while (parent_index > bucket_index);

    parent_index = bucket_index - parent_index;
    
    return parent_index;
}

/* 
 * initialize_bucket: recursively initialize the parent's buckets.
 */
static void initialize_bucket(struct hash_set* hs, int tid, int bucket_index) {
    //First, find the parent bucket.If the parent bucket hasn't be initialized, initialize it.
    int parent_index = get_parent_index(hs, bucket_index);
    struct bucket_list* parent_bucket = get_bucket_list(hs, parent_index);
	struct bucket_list* new_bucket;

    while (parent_bucket == NULL) {
        initialize_bucket(hs, tid, parent_index);  //recursive call.
        parent_bucket = get_bucket_list(hs, parent_index);
    }

    //Second, find the insert point in the parent bucket's linked_list and use CAS to insert new_bucket->bucket_sentinel->ll_node into the parent bucket.
    
    //Third, allocate a new bucket_list here.
    bucket_list_init(&new_bucket, bucket_index);

    //FIXME:We want to insert the a allocated sentinel node into the parent bucket_list. But ll_insert() will also malloc a new memory area. It't duplicated.
    if (ll_insert_node(&parent_bucket->bucket_sentinel, tid, &(new_bucket->bucket_sentinel.ll_head)) == 0) {
    //if (ll_insert_ready_made(&parent_bucket->bucket_sentinel, tid, &(new_bucket->bucket_sentinel.ll_head)) == 0) {
        //success!
        set_bucket_list(hs, tid, bucket_index, new_bucket);
    } else {
        //FIXME:encapsulate a function to free a struct bucket_list.
        free(new_bucket);  //ugly.use function.
    }
}

static int __hs_insert_small(struct hash_set *hs, pkey_t key, pval_t* val) {
    pkey_t old;
    size_t i;

    /* TODO: Avoid no empty slot case. */
    for (i = hash_64(key, SMALL_HASHSET_SLOT_SFT); ; i = (i + 1) % SMALL_HASHSET_SLOTN) {
        old = hs->slots[i].k;
        if (val && old == EMPTY_SLOT_KEY) {
            old = cmpxchg(&hs->slots[i].k, EMPTY_SLOT_KEY, key);
        }
        if (old == EMPTY_SLOT_KEY || old == key) {
            break;
        }
    }

    if (val) {
        /* Upsertion */
        hs->slots[i].v = val;
        if (old == EMPTY_SLOT_KEY) {
            xadd(&hs->set_size, 1);
            return 0;
        } else {
            return -EEXIST;
        }
    } else {
        /* Deletion */
        if (unlikely(old == EMPTY_SLOT_KEY)) {
            return -ENOENT;
        } else {
            hs->slots[i].v = NULL;
            xadd(&hs->set_size, -1);
            return 0;
        }
    }
}

static int __hs_remove_small(struct hash_set *hs, pkey_t key) {
    return __hs_insert_small(hs, key, NULL);
}

static pval_t *__hs_lookup_small(struct hash_set *hs, pkey_t key) {
    pkey_t probe;
    size_t i;
    for (i = hash_64(key, SMALL_HASHSET_SLOT_SFT); ; i = (i + 1) % SMALL_HASHSET_SLOTN) {
        probe = hs->slots[i].k;
        if (probe == key) {
            return hs->slots[i].v;
        } else if (probe == EMPTY_SLOT_KEY) {
            return NULL;
        }
    }
}

static void __hs_scan_and_ops_small(struct hash_set* hs, hs_exec_t exec, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
    int i;
    struct ll_node tmp;
    for (i = 0; i < SMALL_HASHSET_SLOTN; i++) {
        kvpair_t *kv = &hs->slots[i];
        if (kv->k != EMPTY_SLOT_KEY && kv->v) {
            tmp.key = reverse(kv->k);
            tmp.val = kv->v;
            exec(&tmp, arg1, arg2, arg3, arg4, arg5);
        }
    }
}

static void hs_avg_insert(struct hash_set *hs, pkey_t key) {
    union atomic_u128 old_avg, new_avg;
    while(1) {
        if (unlikely(key == ULONG_MAX)) {
            break;
        }
        old_avg = (union atomic_u128) AtomicLoad128(&hs->avg);
        new_avg.hi = old_avg.hi + 1;
        new_avg.lo = (old_avg.lo * old_avg.hi + key) / new_avg.hi;
        if (AtomicCAS128(&hs->avg, &old_avg, new_avg)) {
            break;
        }
    }
}

static void hs_avg_remove(struct hash_set *hs, pkey_t key) {
    union atomic_u128 old_avg, new_avg;
    while(1) {
        old_avg = (union atomic_u128) AtomicLoad128(&hs->avg);
        new_avg.hi = old_avg.hi - 1;
        new_avg.lo = (old_avg.lo * old_avg.hi - key) / new_avg.hi;
        if (AtomicCAS128(&hs->avg, &old_avg, new_avg)) {
            break;
        }
    }
}

static void hs_transform(struct hash_set *hs, int tid, int to_small);

static int __hs_insert(struct hash_set* hs, int tid, pkey_t key, pval_t* val, int update) {
    //First, calculate the bucket index by key and capacity_order of the hash_set.
    int bucket_index = key & MASK(hs->capacity_order);
    struct bucket_list* bucket = get_bucket_list(hs, bucket_index);  //bucket will be initialized if needed in get_bucket_list()
	unsigned long capacity_order_now, capacity_now, old_capacity_order;
	int set_size_now;
    int ret = 0;

    while (bucket == NULL) {
        //FIXME: It seems that the bucket will not always be initialized as we expected.
        initialize_bucket(hs, tid, bucket_index);
        bucket = get_bucket_list(hs, bucket_index);
    }
	
    ret = ll_insert(&bucket->bucket_sentinel, tid, make_ordinary_key(key), val, update);
    if (!update && ret == -EEXIST) {
        // fail to insert the key into the hash_set
        // bonsai_debug("thread [%d]: hs_insert(%lu) fail!\n", tid, key);
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&hs_add_fail_time, 1);
#endif
        return -EEXIST;
    }
    
    //bonsai_debug("thread [%d]: hs_insert(%lu) success!\n", tid, key);
	
#ifdef BONSAI_HASHSET_DEBUG
    xadd(&hs_node_count, 1);
#endif

	set_size_now = (ret != -EEXIST) ? xadd(&hs->set_size, 1) : hs->set_size;

#ifdef BONSAI_HASHSET_DEBUG
    xadd(&hs_add_success_time, 1);
#endif
    capacity_order_now = hs->capacity_order;
    capacity_now = 1ul << capacity_order_now;  //we must fetch and store the value before test whether of not to resize. Be careful don't resize multi times.
 
    //Do we need to resize the hash_set?
    if (set_size_now >= (hs->load_factor << capacity_order_now)) {
        if (capacity_now * 2 <= MAIN_ARRAY_LEN * SEGMENT_SIZE) {
            old_capacity_order = cmpxchg(&hs->capacity_order, capacity_order_now, capacity_order_now + 1);
            if (old_capacity_order == capacity_order_now) {
				;
                //bonsai_debug("[%d %lu] resize succeed [%lu]\n", set_size_now, capacity_now, hs->capacity_order);
            }
        } else {
            // perror("cannot resize, the buckets number reaches (MAIN_ARRAY_LEN * SEGMENT_SIZE).\n");
        }
    }
    return ret;
}

static pval_t* __hs_lookup(struct hash_set* hs, int tid, pkey_t key) {
    // First, get bucket index.
    // Note: it is a corner case. If the hs is resized not long ago. Some elements should be adjusted to new bucket,
    // When we find a key who is in the hs, but haven't been "moved to" the new bucket. We need to "move" it.
    // Actually, we need to initialize the new bucket and insert it into the hs main_list.
    int bucket_index = key & MASK(hs->capacity_order);
    struct bucket_list* bucket = get_bucket_list(hs, bucket_index);
	pval_t* addr = NULL;

    while (bucket == NULL) {
        initialize_bucket(hs, tid, bucket_index);
        bucket = get_bucket_list(hs, bucket_index);
    }

    addr = bucket_list_lookup(bucket, tid, make_ordinary_key(key));

	return addr;
}

static int __hs_remove(struct hash_set* hs, int tid, pkey_t key) {
    // First, get bucket index.
    //if the hash_set is resized now! Maybe we cannot find the new bucket. Just initialize it if the bucket if NULL.
    int bucket_index = key & MASK(hs->capacity_order);
    struct bucket_list* bucket = get_bucket_list(hs, bucket_index);
	int ret;

    if (bucket == NULL) {
        initialize_bucket(hs, tid, bucket_index);
        bucket = get_bucket_list(hs, bucket_index);
    }

    //origin_sentinel_key = get_origin_key(bucket->bucket_sentinel.ll_head.key);
    //bonsai_debug("key - %d 's sentinel origin key is %u.\n", key, origin_sentinel_key);
    // Second, remove the key from the bucket.
    // the node will be taken over by bucket->bucket_sentinel's HP facility. I think it isn't efficient but can work.
    ret = ll_remove(&bucket->bucket_sentinel, tid, make_ordinary_key(key));
    if (ret == 0) {
        xadd(&hs->set_size, -1);
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&hs_remove_success_time, 1);
#endif
        //bonsai_debug("remove %d success!\n", key);
    }
#ifdef BONSAI_HASHSET_DEBUG
	else {
        xadd(&hs_remove_fail_time, 1);
    }
#endif
}

static int hs_update_unlocked(struct hash_set* hs, int tid, pkey_t key, pval_t* val) {
	int ret;
    if (hs->small) {
        if (unlikely(hs->set_size > SMALL_HASHSET_MAXN)) {
            return -ENOMEM;
        }
        ret = __hs_insert_small(hs, key, val);
    } else {
        ret = __hs_insert(hs, tid, key, val, 1);
    }
    if (!ret) {
        hs_avg_insert(hs, key);
    }
    return ret;
}

static int hs_insert_unlocked(struct hash_set* hs, int tid, pkey_t key, pval_t* val) {
	int ret;
    if (hs->small) {
        if (unlikely(hs->set_size > SMALL_HASHSET_MAXN)) {
            return -ENOMEM;
        }
        ret = __hs_insert_small(hs, key, val);
    } else {
        ret = __hs_insert(hs, tid, key, val, 0);
    }
    if (!ret) {
        hs_avg_insert(hs, key);
    }
    return ret;
}

static pval_t* hs_lookup_unlocked(struct hash_set* hs, int tid, pkey_t key) {
    pval_t *ret;
    if (hs->small) {
        ret = __hs_lookup_small(hs, key);
    } else {
        ret = __hs_lookup(hs, tid, key);
    }
    return ret;
}

static int hs_remove_unlocked(struct hash_set* hs, int tid, pkey_t key) {
    int ret;
    if (hs->small) {
        ret = __hs_remove_small(hs, key);
    } else {
        ret = __hs_remove(hs, tid, key);
        if (!ret && hs->set_size < SMALL_HASHSET_MAXN) {
            ret = -ENOMEM;
        }
    }
    if (!ret) {
        hs_avg_remove(hs, key);
    }
    return ret;
}

static void hs_scan_and_ops_unlocked(struct hash_set* hs, hs_exec_t exec, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	struct bucket_list **buckets, *bucket;
	segment_t* p_segment;
	struct ll_node *head, *curr;
	int i, j;

    if (hs->small) {
        __hs_scan_and_ops_small(hs, exec, arg1, arg2, arg3, arg4, arg5);
        return;
    }

	for (i = 0; i < MAIN_ARRAY_LEN; i++) {
		p_segment = hs->main_array[i];
        if (p_segment == NULL)
            continue;

		for (j = 0; j < SEGMENT_SIZE; j++) {
			buckets = (struct bucket_list**)p_segment;
			bucket = buckets[j];
			if (bucket == NULL)
                continue;

            head = &(bucket->bucket_sentinel.ll_head);
			curr = GET_NODE(head->next);
			while (curr) {
                if (is_sentinel_node(curr)) {
                   	 break;
                } else {
					exec(curr, arg1, arg2, arg3, arg4, arg5);
                }
                curr = GET_NODE(STRIP_MARK(curr->next));
            }
		}
	}
}

/*
 * hs_update: return 0 if key does not exists else return -EEXIST
 */
int hs_update(struct hash_set* hs, int tid, pkey_t key, pval_t* val) {
	int ret;
retry:
    read_lock(&hs->transform_lock);
    ret = hs_update_unlocked(hs, tid, key, val);
    if (ret == -ENOMEM) {
        read_unlock(&hs->transform_lock);
        hs_transform(hs, tid, 0);
        goto retry;
    }
    read_unlock(&hs->transform_lock);
    return ret;
}

/*
 * hs_insert: return 0 if succeed else return -EEXIST
 */
int hs_insert(struct hash_set* hs, int tid, pkey_t key, pval_t* val) {
	int ret;
retry:
    read_lock(&hs->transform_lock);
    ret = hs_insert_unlocked(hs, tid, key, val);
    if (ret == -ENOMEM) {
        read_unlock(&hs->transform_lock);
        hs_transform(hs, tid, 0);
        goto retry;
    }
    read_unlock(&hs->transform_lock);
    return ret;
}

/* 
 * hs_lookup: return 1 if hs contains key, 0 otherwise.
 */
pval_t* hs_lookup(struct hash_set* hs, int tid, pkey_t key) {
    pval_t *ret;
    read_lock(&hs->transform_lock);
    ret = hs_lookup_unlocked(hs, tid, key);
    read_unlock(&hs->transform_lock);
    return ret;
}

/*
 * hs_remove: 0 if succeed, -1 if fail.
 */
int hs_remove(struct hash_set* hs, int tid, pkey_t key) {
    int ret;
    read_lock(&hs->transform_lock);
    ret = hs_remove_unlocked(hs, tid, key);
    if (ret == -ENOMEM) {
        ret = 0;
        read_unlock(&hs->transform_lock);
        hs_transform(hs, tid, 1);
        goto out;
    }
    read_unlock(&hs->transform_lock);
out:
    return ret;
}

static void fetch_kv(struct ll_node *node, kvpair_t *kvpairs, int *n, void *arg3, void *arg4, void *arg5) {
    kvpairs[(*n)++] = (kvpair_t) { reverse(node->key), node->val };
}

static kvpair_t *fetch_kvs(struct hash_set *hs, int *n) {
    kvpair_t *kvpairs = malloc(sizeof(kvpair_t) * hs->set_size);
    *n = 0;
    hs_scan_and_ops_unlocked(hs, fetch_kv, kvpairs, n, NULL, NULL, NULL);
    return kvpairs;
}

static void hs_transform(struct hash_set *hs, int tid, int to_small) {
    kvpair_t *kvpairs;
    int n, i;

    write_lock(&hs->transform_lock);

    if (to_small == hs->small) {
        goto out;
    }

    if (unlikely(to_small && hs->set_size > SMALL_HASHSET_MAXN)) {
        goto out;
    }

    kvpairs = fetch_kvs(hs, &n);

    hs_destroy(hs);
    hs->small = to_small;
    hs_init(hs);

    for (i = 0; i < n; i++) {
        hs_insert_unlocked(hs, tid, kvpairs[i].k, kvpairs[i].v);
    }

    free(kvpairs);

out:
    write_unlock(&hs->transform_lock);
}

#ifdef BONSAI_HASHSET_DEBUG
//[sentinel]->(ordinary)->(ordinary)->[sentinel]
void hs_print(struct hash_set* hs, int tid) {
    int print_count = 0;
    int ordinary_count = 0;
    //we need to traverse from the bucket-0's sentinel ll_node to the end.
    struct ll_node* head = &(get_bucket_list(hs, 0)->bucket_sentinel.ll_head);  //now head is the head of the main-list.
    struct ll_node* curr = head;
    while (curr) {
        xadd(&print_count, 1);
        if (is_sentinel_node(curr)) {
            bonsai_debug("[%d] -> ", get_origin_key(curr->key));
        } else {
            if (!HAS_MARK(curr->next)) {
                xadd(&ordinary_count, 1);
                bonsai_debug("(%d)%c -> ", get_origin_key(curr->key), (HAS_MARK(curr->next) ? '*' : ' '));
            }
        }
        
        curr = GET_NODE(STRIP_MARK(curr->next));
    } 
    bonsai_debug("NULL.\n");
    bonsai_debug("hash_set : set_size = %d.\n", hs->set_size);
    if (hs->set_size != ordinary_count) {
        bonsai_debug("FAIl! (hs->set_size != ordinary_count)\n");
    } else {
        bonsai_debug("SUCCESS! (hs->set_size == ordinary_count)\n");
    }
    
    bonsai_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity_order);
    bonsai_debug("print_count = %d.\n", print_count);
    
}

/*
 * hs_print_through_bucket: traverse the hs->main_array and print each bucket_list.
 */
void hs_print_through_bucket(struct hash_set* hs, int tid) {
	struct bucket_list **buckets, *bucket;
	struct ll_node *head, *curr;
	int i, j; 

    for (i = 0; i < MAIN_ARRAY_LEN; i++) {
        segment_t* p_segment = hs->main_array[i];
        if (p_segment == NULL) {
            continue;
        }
        for (j = 0; j < SEGMENT_SIZE; j++) {
            buckets = (struct bucket_list**)p_segment;
            bucket = buckets[j];
            if (bucket == NULL) {
                continue;
            }
            //now traverse the bucket.
            head = &(bucket->bucket_sentinel.ll_head);
            bonsai_debug("[%d]%c -> ", get_origin_key(head->key), (HAS_MARK(head->next) ? '*' : ' '));

            curr = GET_NODE(head->next);
            while (curr) {
                if (is_sentinel_node(curr)) {
                    bonsai_debug("NULL\n");
                    break;
                } else {
                    bonsai_debug("(%d)%c -> ", get_origin_key(curr->key), (HAS_MARK(curr->next) ? '*' : ' '));
                }
                curr = GET_NODE(STRIP_MARK(curr->next));
            } 
            if (curr == NULL) {
                bonsai_debug("NULL\n");
            }
        }
    }
    
    bonsai_debug("hash_set : set_size = %d.\n", hs->set_size);
    bonsai_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity_order);
}
#endif

static pkey_t hs_copy_one(struct ll_node* node, struct hash_set *new, struct hash_set *mid, 
		pkey_t max, pkey_t avg_key, struct pnode* new_pnode, struct pnode* mid_pnode) {
	pval_t* old;
	int tid = get_tid();
	pkey_t key;

	key = get_origin_key(node->key);
    if (mid_pnode && key_cmp(key, avg_key) <= 0) {
        if (key_cmp(key, max) <= 0) {
            old = node->val;
            if (addr_in_log((unsigned long)old)) {
                hs_insert(new, tid, key, old);
                return key;
            } else if (addr_in_pnode((unsigned long)old)) {
                old = pnode_lookup(new_pnode, key);
                // if (!old) {
                //     data_layer_search_key(key);
                //     print_pnode(new_pnode);
                //     assert(0);
                // }
                hs_insert(new, tid, key, old);
                return key;		
            } else {
                printf("%lu %016lx\n", key, old);
                perror("invalid address\n");
                assert(0);
            }
        } else {
            old = node->val;
            if (addr_in_log((unsigned long)old)) {
                hs_insert(mid, tid, key, old);
                return key;
            } else if (addr_in_pnode((unsigned long)old)) {
                old = pnode_lookup(mid_pnode, key);
                // if (!old) {
                //     // stop_the_world();
                //     data_layer_search_key(key);
                //     printf("avg_key: %lu\n", avg_key);
                //     print_pnode(mid_pnode);
                //     print_pnode(mid_pnode->table->forward->pnode);
                //     // dump_pnode_list_summary();
                //     assert(0);
                // }
                hs_insert(mid, tid, key, old);
                return key;		
            } else {
                perror("invalid address\n");
                assert(0);
            }
        }
    } else {
        if (key_cmp(key, max) <= 0) {
            old = node->val;
            if (addr_in_log((unsigned long)old)) {
                hs_insert(new, tid, key, old);
                return key;
            } else if (addr_in_pnode((unsigned long)old)) {
                old = pnode_lookup(new_pnode, key);
                if (!old) {
                    data_layer_search_key(key);
                    print_pnode(new_pnode);
                    assert(0);
                }
                //assert(old);
                hs_insert(new, tid, key, old);
                return key;		
            } else {
                perror("invalid address\n");
                assert(0);
            }
        }
    }
	
	return -2;
}

typedef struct {
    struct hash_set *new, *mid;
    pkey_t max, avg_key;
    struct pnode *new_pnode, *mid_pnode;
} copy_opt_t;

static void do_copy_one(struct ll_node *curr, void *arg1, void *arg2, void *arg3, void *arg4, void *arg5) {
	struct flush_work* fwork = (struct flush_work*)__this->t_work->exec_arg;
	int *use_big = arg2;
	pkey_t key;
    copy_opt_t *opt = arg1;

    key = hs_copy_one(curr, opt->new, opt->mid, opt->max, opt->avg_key, opt->new_pnode, opt->mid_pnode);
    if (key != (unsigned long)-2) {
        if (!*use_big && fwork->small_free_cnt < SEGMENT_SIZE)
            fwork->small_free_set[fwork->small_free_cnt++] = key;
        if (fwork->small_free_cnt == SEGMENT_SIZE) {
            *use_big = 1;
        }
        if (*use_big)
            fwork->big_free_set[fwork->big_free_cnt++] = key;
    }
}

void hs_scan_and_split(struct hash_set *old, struct hash_set *new, struct hash_set *mid,
			pkey_t max, pkey_t avg_key, struct pnode* new_pnode, struct pnode* mid_pnode) {
	struct bucket_list **buckets, *bucket;
	segment_t* p_segment;
	struct ll_node *head, *curr;
	int i, j, tid = get_tid();
	int use_big = 0;
	struct flush_work* fwork = (struct flush_work*)__this->t_work->exec_arg;
    copy_opt_t opt = { new, mid, max, avg_key, new_pnode, mid_pnode };

    hs_scan_and_ops(old, do_copy_one, &opt, &use_big, NULL, NULL, NULL);

	for (i = 0; i < fwork->small_free_cnt; i ++)
		hs_remove(old, tid, fwork->small_free_set[i]);

	fwork->small_free_cnt = 0;

	if (use_big) {
		for (i = 0; i < fwork->big_free_cnt; i ++)
			hs_remove(old, tid, fwork->big_free_set[i]);

		fwork->big_free_cnt = 0;
	}
}

void hs_search_key(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	pkey_t key, target = (pkey_t)arg1;
	pval_t* addr;

	key = get_origin_key(node->key);
	if (!key_cmp(key, target)) {
		addr = node->val;
		bonsai_print("hash set search key[%lu]: address: %016lx <%lu %lu>\n", key, addr, key, *addr);
	}
}

void hs_print_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	pkey_t key = get_origin_key(node->key);
	bonsai_print("<%lu %lu> ", key, *node->val);
}

void hs_count_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	int* total = (int*)arg1;
	pkey_t* max = (pkey_t*)arg2;
	pkey_t key = get_origin_key(node->key);
	
	*total += 1;
	if (key_cmp(key, *max) > 0)
		*max = key;
}

void hs_check_entry(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	pkey_t key = get_origin_key(node->key);
	pval_t* addr = node->val;
	struct oplog* log;

	if (addr_in_log((unsigned long)addr)) {
		log = OPLOG(addr);
		if (key_cmp(key, log->o_kv.k) != 0) 
			bonsai_print("bad hs entry: *%lu* <%lu %lu>\n", key, log->o_kv.k, log->o_kv.v);
	}
}

void hs_scan_and_ops(struct hash_set* hs, hs_exec_t exec, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
    read_lock(&hs->transform_lock);
    hs_scan_and_ops_unlocked(hs, exec, arg1, arg2, arg3, arg4, arg5);
    read_unlock(&hs->transform_lock);
}

void hs_destroy(struct hash_set* hs) { 
    // First , get the first linked_list of bucket-0.
    struct bucket_list** buckets_0;
    struct linked_list* ll_curr;  // Now, ll_curr is the most left linked_list.
	struct bucket_list* bucket;
	struct ll_node *p1, *p2;
	segment_t* segment_curr;
	int bucket_index, i;

    if (hs->small) {
        return;
    }

    buckets_0 = (struct bucket_list**)hs->main_array[0];
    ll_curr = &(buckets_0[0]->bucket_sentinel);

    while (1) {
        p1 = &(ll_curr->ll_head);
        p2 = GET_NODE(STRIP_MARK(p1->next));

        while (p2 != NULL && !is_sentinel_node(p2)) {
            p1 = p2;
            p2 = GET_NODE(STRIP_MARK(p1->next));
        }

        if (p2 == NULL) {
            // ll_curr must be the last linked_list in the main list. Just call ll_destroy() to destroy it.
            ll_destroy(ll_curr);
            break;
        } else {
            // p2 is the head(sentinel) of a linked_list.
            p1->next = 0;
            ll_destroy(ll_curr);
            // Now the ll_curr is destroyed, we need to find the new ll_curr started from the p2.
            // We need to find the linked_list from the main_array using p2->key as the index.
            bucket_index = get_origin_key(p2->key);
            bucket = get_bucket_list(hs, bucket_index);   // tid is an arbitrary value. bucket must not be NULL.
            ll_curr = &(bucket->bucket_sentinel);
        }
    }

    // Second, we have freed the whole linked_list, now we need to free all of the segments.
    for (i = 0; i < MAIN_ARRAY_LEN; i ++) {
        segment_curr = hs->main_array[i];
        if (segment_curr == NULL) continue;
        free(segment_curr);
    }
	
#ifdef BONSAI_HASHSET_DEBUG
    bonsai_debug("hs_add_success_time = %d, hs_add_fail_time = %d.\n hs_remove_success_time = %d, hs_remove_fail_time = %d.\n", 
		hs_add_success_time, hs_add_fail_time, hs_remove_success_time, hs_remove_fail_time);
    bonsai_debug("hs_node_count = %d.\n", hs_node_count);
#endif
}
