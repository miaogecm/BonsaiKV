/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *	   	   Shenming Liu
 *
 * A lock-free concurrent link list implementation with safe memory reclaimation.
 */
#include <string.h>

#include "link_list.h"
#include "hp.h"

static int node_count = 0;
static int retire_count = 0;
static int print_count = 0;

#define IS_TAGGED(v, tag)    ((v) &  tag) 
#define STRIP_TAG(v, tag)    ((v) & ~tag)
#define TAG_VALUE(v, tag)    ((v) |  tag)

#define HAS_MARK(markable_t_value)          (IS_TAGGED((markable_t_value), 0x1) == 0x1)
#define STRIP_MARK(markable_t_value)        (STRIP_TAG((markable_t_value), 0x1))
#define MARK_NODE(markable_t_value)         (TAG_VALUE(markable_t_value, 0x1))
#define GET_NODE(markable_t_value)          ((struct ll_node*)markable_t_value) 

#define SYNC_SWAP(addr,x)         __sync_lock_test_and_set(addr,x)
#define SYNC_CAS(addr,old,x)      __sync_val_compare_and_swap(addr,old,x)
#define SYNC_ADD(addr,n)          __sync_add_and_fetch(addr,n)
#define SYNC_SUB(addr,n)          __sync_sub_and_fetch(addr,n)

void ll_init(struct linked_list* ll) {
    memset(ll, 0, sizeof(struct linked_list));
}

struct ll_node* ll_find(struct linked_list* ll, int tid, pkey_t key, 
		struct ll_node** __pred, struct ll_node** __succ) {
	struct ll_node *pred, *curr, *succ;
    struct hp_item* hp = ll->HP[tid];

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);

retry: 
    {
        pred = &(ll->ll_head);  //iterate the linked_list from it's head sentinel node.
        curr = GET_NODE(pred->next);
        while (curr) {
            hp_save_addr(hp, 0, (hp_t)curr);  //protect curr's address.
            //then we need to validate whether curr is safe.[1].pred should not be marked. [2].pred-->curr
            if (HAS_MARK((markable_t)pred) || pred->next != (markable_t)curr) {
                hp_clear_all_addr(hp);
                goto retry;
            }
            markable_t next = curr->next;
            if (HAS_MARK(next)) {
                //curr is marked, we need to physically remove it.
                succ = GET_NODE(STRIP_MARK(next));
                //[1]. pred must not be marked. [2]. pred--->curr should not be changed.
                markable_t old_value = SYNC_CAS(&pred->next, curr, succ);
                if (old_value != (markable_t)curr) {
                    //CAS failed.
                    goto retry;  //retry.
                }
                //CAS succeed. Go forwards.
                
                //=======================retire the node curr===========================
                hp_retire_node(ll, hp, (hp_t)curr);
                SYNC_ADD(&retire_count, 1);
                curr = succ;
            } else {
                succ = GET_NODE(STRIP_MARK(next));
                if (curr->key >= key) {
                    if (__succ) *__succ = curr;
                    if (__pred) *__pred = pred;
                    return curr;
                }
                //haven't found the target. Go forwards.
                pred = curr;
                hp_t hp0 = hp_get_addr(hp, 0);
                hp_save_addr(hp, 1, hp0);
                curr = succ;
            }
        }
        if (__succ) *__succ = curr;
        if (__pred) *__pred = pred;
        return NULL;
    }
}

int ll_insert(struct linked_list* ll, int tid, pkey_t key, pval_t val) {
	struct ll_node *pred, *item, *succ;
    struct hp_item* hp = ll->HP[tid];

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);
    
    while (1) {
        pred = NULL, succ = NULL;
        item = ll_find(ll, tid, key, &pred, &succ);
        if (item && item->key == key) {
            //key is now in the linked list.
            hp_clear_all_addr(hp);
            return -EEXIST;
        }
        item = (struct ll_node*) malloc (sizeof(struct ll_node));
    
        item->key = key;
        item->next = (markable_t)succ;
    
        //[1]. pred must not be marked. [2]. pred--->succ should not be changed.
        markable_t old_value = SYNC_CAS(&pred->next, succ, item);
        if (old_value != (markable_t)succ) {
            //CAS failed!
            free(item);
            continue;
        }
        //CAS succeed!
        SYNC_ADD(&node_count, 1);
        hp_clear_all_addr(hp);
        return 0;
    }
}


int ll_remove(struct linked_list* ll, int tid, pkey_t key) {
	struct ll_node *pred, *item;
    struct hp_item* hp = ll->HP[tid];

    if (hp == NULL) 
		hp = hp_item_setup(ll, tid);
    
    pred = NULL;
    markable_t old_value;
    while (1) {
        item = ll_find(ll, tid, key, &pred, NULL);
        if (!item || item->key != key) {
            //cannot find key int the ll.
            hp_clear_all_addr(hp);
            return -EEXIST;
        } else {
            //logically remove the item.
            //try to mark item.
            markable_t next = item->next;
            old_value = SYNC_CAS(&item->next, next, MARK_NODE(next));
            if (old_value != next) {
                //fail to mark item. Now item could be marked by others OR removed by others OR freed by others OR still there.
                //so we need to retry from list head.
                continue;
            }
            
            ll_find(ll, tid, key, NULL, NULL);
            hp_clear_all_addr(hp);
            return 0;
        }
    }
}

int ll_lookup(struct linked_list* ll, int tid, pkey_t key) {
    struct hp_item* hp = ll->HP[tid];
	struct ll_node* curr;

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);

    ll_find(ll, tid, key, NULL, &curr);
 
    if (curr && curr->key == key) {
        hp_clear_all_addr(hp);
        return 1;
    }
    hp_clear_all_addr(hp);
    return 0;
}


void ll_print(struct linked_list* ll) {
    struct ll_node* head = &(ll->ll_head);
    struct ll_node* curr = GET_NODE(head->next);

    printf("[head] -> ");
    while (curr) {
        SYNC_ADD(&print_count, 1);
        printf("[%d]%c -> ", curr->key, (HAS_MARK(curr->next) ? '*' : ' '));
        curr = GET_NODE(curr->next);
    } 

    printf("NULL.\n");
    printf("print_count = %d.\n", print_count);
}

void ll_destroy(struct linked_list* ll) {
	struct ll_node *head, *curr;
	struct ll_node* old_curr;

    hp_setdown(ll);
    head = &(ll->ll_head);
    curr = GET_NODE(head->next);

    while (curr) {
        old_curr = curr;
        curr = GET_NODE(curr->next);
        free(old_curr);
        SYNC_SUB(&node_count, 1);
    } 

    free(head);
    printf("node_count = %d.\n", node_count);
    printf("retire_count = %d.\n", retire_count);

    retire_count = 0;
    node_count = 0;
    print_count = 0;
}
