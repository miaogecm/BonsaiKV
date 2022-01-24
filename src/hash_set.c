/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *	   	   Shenming Liu
 *
 * A lock-free extendible hash table implementation. 
 * Split-Ordered Lists: Lock-Free Extensible Hash Tables. Ori Shalev, Nir Shavit. Journal of the ACM, Vol.53, No.3, 2006, pp.379�405
 *
 * A lock-free hash table consists of two
 * components: 
 * (1) a lock-free linked list
 * (2) an expanding array of references into the list
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <thread.h>
#include <assert.h>

#include "hash_set.h"
#include "common.h"
#include "atomic.h"
#include "pnode.h"

#define MASK 	0x00FFFFFFFFFFFFFF  /* takes the lowest 3 bytes of the hashcode */
#define HI_MASK 0x8000000000000000  /* the MSB of a unsigned long value */

#ifdef BONSAI_HASHSET_DEBUG
static int hs_node_count = 0;
static int hs_add_success_time = 0;
static int hs_add_fail_time = 0;
static int hs_remove_success_time = 0;
static int hs_remove_fail_time = 0;
#endif

static void bucket_list_init(struct bucket_list** bucket, int bucket_index);
static struct bucket_list* get_bucket_list(struct hash_set* hs, int bucket_index);
static void set_bucket_list(struct hash_set* hs, int tid, int bucket_index, struct bucket_list* new_bucket);
static void initialize_bucket(struct hash_set* hs, int tid, int bucket_index);
static int get_parent_index(struct hash_set* hs, int bucket_index);

static pkey_t reverse(pkey_t code) {
    pkey_t res = 0;
	int i, size = sizeof(pkey_t) * 8;

    for (i = 0; i < size; ++i) {
            res |= ((code >> i) & 1) << (size - 1 - i);
        }
    return res;
}

static inline pkey_t hash(pkey_t key) {
    return key;
}

/*
 * make_ordinary_key: set MSB to 1, and reverse the key.
 */
static pkey_t make_ordinary_key(pkey_t key) {
    pkey_t code = hash(key) & MASK;
    return reverse(code | HI_MASK);  //mark the MSB and reverse it.
}

/* 
 * make_sentinel_key: set MSB to 0, and reverse the key.
 */
static pkey_t make_sentinel_key(pkey_t key) {
    return reverse(key & MASK);
}

/*
 * is_sentinel_key: 1 if key is sentinel key, 0 if key is ordinary key.
 */
static int is_sentinel_key(pkey_t key) {
    if (key == ((key >> 1) << 1)) {
        return 1;
    }
    return 0;
}

static pkey_t get_origin_key(pkey_t key) {
    key = (key >> 1) << 1;
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
        	;
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
}

void hs_init(struct hash_set* hs) {
	struct bucket_list** segment;

    memset(hs->main_array, 0, MAIN_ARRAY_LEN * sizeof(segment_t*));  //important.
    hs->load_factor = LOAD_FACTOR_DEFAULT;  // 2
    hs->capacity = INIT_NUM_BUCKETS;  // 2 at first
    hs->set_size = 0;
	
    get_bucket_list(hs, 0);  //allocate the memory of the very first segment
    segment = (struct bucket_list **)hs->main_array[0];

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
    int parent_index = hs->capacity;

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

    if (parent_bucket == NULL) {
        initialize_bucket(hs, tid, parent_index);  //recursive call.
    }

    //Second, find the insert point in the parent bucket's linked_list and use CAS to insert new_bucket->bucket_sentinel->ll_node into the parent bucket.
    parent_bucket = get_bucket_list(hs, parent_index);
    
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

int hs_insert(struct hash_set* hs, int tid, pkey_t key, pval_t* val) {
    //First, calculate the bucket index by key and capacity of the hash_set.
    int bucket_index = hash(key) % hs->capacity;
    struct bucket_list* bucket = get_bucket_list(hs, bucket_index);  //bucket will be initialized if needed in get_bucket_list()
	unsigned long capacity_now, old_capacity;
	int set_size_now;
    int insert_ret;

    while (bucket == NULL) {
        //FIXME: It seems that the bucket will not always be initialized as we expected.
        initialize_bucket(hs, tid, bucket_index);
        bucket = get_bucket_list(hs, bucket_index);
    }
	
    insert_ret = ll_insert(&bucket->bucket_sentinel, tid, make_ordinary_key(key), val);
    if (insert_ret != 0) {
        // fail to insert the key into the hash_set
        // bonsai_debug("thread [%d]: hs_insert(%lu) fail!\n", tid, key);
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&hs_add_fail_time, 1);
#endif
        // return -1;
    }

    //bonsai_debug("thread [%d]: hs_insert(%lu) success!\n", tid, key);
	
#ifdef BONSAI_HASHSET_DEBUG
    xadd(&hs_node_count, 1);
#endif

	set_size_now = (insert_ret != -EEXIST) ? xadd(&hs->set_size, 1) : hs->set_size;

#ifdef BONSAI_HASHSET_DEBUG
    xadd(&hs_add_success_time, 1);
#endif
    capacity_now = hs->capacity;  //we must fetch and store the value before test whether of not to resize. Be careful don't resize multi times.
 
    //Do we need to resize the hash_set?
    if ((1.0) * set_size_now / capacity_now >= hs->load_factor) {
        if (capacity_now * 2 <= MAIN_ARRAY_LEN * SEGMENT_SIZE) {
            old_capacity = cmpxchg(&hs->capacity, capacity_now, capacity_now * 2);
            if (old_capacity == capacity_now) {
				;
                //bonsai_debug("[%d %lu] resize succeed [%lu]\n", set_size_now, capacity_now, hs->capacity);
            }
        } else {
            perror("cannot resize, the buckets number reaches (MAIN_ARRAY_LEN * SEGMENT_SIZE).\n");
        }
    }
    return 0;
}


/* 
 * hs_lookup: return 1 if hs contains key, 0 otherwise.
 */
pval_t* hs_lookup(struct hash_set* hs, int tid, pkey_t key) {
    // First, get bucket index.
    // Note: it is a corner case. If the hs is resized not long ago. Some elements should be adjusted to new bucket,
    // When we find a key who is in the hs, but haven't been "moved to" the new bucket. We need to "move" it.
    // Actually, we need to initialize the new bucket and insert it into the hs main_list.
    int bucket_index = hash(key) % hs->capacity;
    struct bucket_list* bucket = get_bucket_list(hs, bucket_index);
	pval_t* addr = NULL;

    if (bucket == NULL) {
        initialize_bucket(hs, tid, bucket_index);  
        bucket = get_bucket_list(hs, bucket_index);
    }

    addr = bucket_list_lookup(bucket, tid, make_ordinary_key(key));

	return addr;
}

/*
 * hs_remove: 0 if succeed, -1 if fail.
 */
int hs_remove(struct hash_set* hs, int tid, pkey_t key) {
    // First, get bucket index.
    //if the hash_set is resized now! Maybe we cannot find the new bucket. Just initialize it if the bucket if NULL.
    int bucket_index = hash(key) % hs->capacity;
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

    return ret;
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
        if (is_sentinel_key(curr->key)) {
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
    
    bonsai_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity);
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
                if (is_sentinel_key(curr->key)) {
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
    bonsai_debug("load factor = %f.\n", (1.0) * hs->set_size / hs->capacity);
}
#endif

void hs_split(struct ll_node* node, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	pkey_t key, min, max;
	pval_t* addr;
	int tid = get_tid();
	struct hash_set *old, *new;
	struct pnode* pnode = (struct pnode*)arg5;

	min = (pkey_t)arg1; max = (pkey_t)arg2;
	old = (struct hash_set*)arg3; new = (struct hash_set*)arg4;

	key = get_origin_key(node->key);
	if (key_cmp(key, min) >= 0 && key_cmp(key, max) <= 0) {
		addr = node->val;
		if (addr_in_log(addr)) {
			hs_insert(new, tid, key, addr);
			hs_remove(old, tid, key);
		} else if (addr_in_pnode(addr)) {
			addr = pnode_lookup(pnode, key);
            assert(addr);
			hs_insert(new, tid, key, addr);
            hs_remove(old, tid, key);
		} else {
			perror("invalid address\n");
			assert(0);
		}
	}
}

void hs_search_key(struct ll_node* node, void* arg1) {
	pkey_t key, target = (pkey_t)arg1;
	pval_t* addr;

	key = get_origin_key(node->key);
	if (!key_cmp(get_origin_key(node->key), target)) {
		addr = node->val;
		bonsai_print("hash set search key[%lu]: address: %016lx\n", key, addr);
	}
}

void hs_scan_and_ops(struct hash_set* hs, hs_op_t func, void* arg1, void* arg2, void* arg3, void* arg4, void* arg5) {
	struct bucket_list **buckets, *bucket;
	struct mptable *m;
	segment_t* p_segment;
	struct ll_node *head, *curr;
	int i, j;

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
                if (is_sentinel_key(curr->key)) {
                   	 break;
                } else {
					func(curr, arg1, arg2, arg3, arg4, arg5);
                }
                curr = GET_NODE(STRIP_MARK(curr->next));
            } 
		}
	}
}

void hs_destroy(struct hash_set* hs) { 
    // First , get the first linked_list of bucket-0.
    struct bucket_list** buckets_0 = (struct bucket_list**)hs->main_array[0];
    struct linked_list* ll_curr = &(buckets_0[0]->bucket_sentinel);  // Now, ll_curr is the most left linked_list.
	struct bucket_list* bucket;
	struct ll_node *p1, *p2;
	segment_t* segment_curr;
	int bucket_index, i;

    while (1) {
        p1 = &(ll_curr->ll_head);
        p2 = GET_NODE(STRIP_MARK(p1->next));

        while (p2 != NULL && !is_sentinel_key(p2->key)) {
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
