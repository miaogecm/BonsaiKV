/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *
 * A lock-free concurrent link list implementation with safe memory reclaimation.
 */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "link_list.h"

#ifdef BONSAI_HASHSET_DEBUG
static int node_count = 0;
static int retire_count = 0;
static int print_count = 0;
#endif

void ll_init(struct linked_list* ll) {
    memset(ll, 0, sizeof(struct linked_list));
}

struct ll_node* ll_find(struct linked_list* ll, int tid, pkey_t key, 
		struct ll_node** __pred, struct ll_node** __succ) {
	struct ll_node *pred, *curr, *succ;
    struct hp_item* hp = ll->HP[tid];
	markable_t next, old_value;
	hp_t hp0;

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
            next = curr->next;
            if (HAS_MARK(next)) {
                //curr is marked, we need to physically remove it.
                succ = GET_NODE(STRIP_MARK(next));
                //[1]. pred must not be marked. [2]. pred--->curr should not be changed.
                old_value = cmpxchg(&pred->next, curr, succ);
                if (old_value != (markable_t)curr) {
                    //CAS failed.
                    goto retry;  //retry.
                }
                //CAS succeed. Go forwards.
                
                //=======================retire the node curr===========================
                hp_retire_node(ll, hp, (hp_t)curr);
#ifdef BONSAI_HASHSET_DEBUG
                xadd(&retire_count, 1);
#endif
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
                hp0 = hp_get_addr(hp, 0);
                hp_save_addr(hp, 1, hp0);
                curr = succ;
            }
        }
        if (__succ) *__succ = curr;
        if (__pred) *__pred = pred;
        return NULL;
    }
}

int ll_insert_node(struct linked_list* ll, int tid, struct ll_node* node) {
    struct ll_node *pred, *item, *succ;
    struct hp_item* hp = ll->HP[tid];
	markable_t old_value;
    pkey_t key = node->key; 

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);
    
    while (1) {
        pred = NULL, succ = NULL;
        item = ll_find(ll, tid, key, &pred, &succ);
        if (item && key_cmp(item->key, key) == 0) {
            if (item->is_sentinel_key == 1) {
                //key is now in the linked list.
                hp_clear_all_addr(hp);
                return -EEXIST;
            }
            else {
                succ = item;
                item = pred;
            }
        }
        node->next = (markable_t)succ;
    
        //[1]. pred must not be marked. [2]. pred--->succ should not be changed.
        old_value = cmpxchg(&pred->next, succ, node);
        if (old_value != (markable_t)succ) {
            //CAS failed!
            continue;
        }
        //CAS succeed!
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&node_count, 1);
#endif
        hp_clear_all_addr(hp);
        return 0;
    }
}

int ll_insert(struct linked_list* ll, int tid, pkey_t key, pval_t* val, int update) {
	struct ll_node *pred, *item, *succ;
    struct hp_item* hp = ll->HP[tid];
	markable_t old_value;

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);
    
    while (1) {
        pred = NULL, succ = NULL;
        item = ll_find(ll, tid, key, &pred, &succ);
        if (item && key_cmp(item->key, key) == 0 && item->is_sentinel_key == 0) {
            //key is now in the linked list.
            hp_clear_all_addr(hp);
			if (update) {
                item->val = val;
            }
			
			return -EEXIST;     
        }
        item = (struct ll_node*) malloc(sizeof(struct ll_node));
    
        item->key = key;
		item->val = val;
        item->next = (markable_t)succ;
        item->is_sentinel_key = 0;
    
        //[1]. pred must not be marked. [2]. pred--->succ should not be changed.
        old_value = cmpxchg(&pred->next, succ, item);
        if (old_value != (markable_t)succ) {
            //CAS failed!
            free(item);
            continue;
        }
        //CAS succeed!
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&node_count, 1);
#endif
        hp_clear_all_addr(hp);
        return 0;
    }
}


int ll_remove(struct linked_list* ll, int tid, pkey_t key) {
	struct ll_node *pred, *item;
    struct hp_item* hp = ll->HP[tid];
	markable_t next, old_value;

    if (hp == NULL) 
		hp = hp_item_setup(ll, tid);
    
    pred = NULL;
    while (1) {
        item = ll_find(ll, tid, key, &pred, NULL);
        if (!item || key_cmp(item->key, key) != 0) {
            //cannot find key int the ll.
            hp_clear_all_addr(hp);
            return -EEXIST;
        } else {
            //logically remove the item.
            //try to mark item.
            next = item->next;
			old_value = cmpxchg(&item->next, next, MARK_NODE(next));
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

struct ll_node* ll_lookup(struct linked_list* ll, int tid, pkey_t key) {
    struct hp_item* hp = ll->HP[tid];
	struct ll_node* curr;

    if (hp == NULL)
		hp = hp_item_setup(ll, tid);

    ll_find(ll, tid, key, NULL, &curr);
 
    if (curr && key_cmp(curr->key, key) == 0) {
        hp_clear_all_addr(hp);
        return curr;
    }
    hp_clear_all_addr(hp);
    return NULL;
}

#ifdef BONSAI_HASHSET_DEBUG
void ll_print(struct linked_list* ll) {
    struct ll_node* head = &(ll->ll_head);
    struct ll_node* curr = GET_NODE(head->next);

    bonsai_debug("[head] -> ");
    while (curr) {
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&print_count, 1);
#endif
        bonsai_debug("[%lu]%c -> ", curr->key, (HAS_MARK(curr->next) ? '*' : ' '));
        curr = GET_NODE(curr->next);
    } 

    bonsai_debug("NULL.\n");
    bonsai_debug("print_count = %d.\n", print_count);
}
#endif

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
#ifdef BONSAI_HASHSET_DEBUG
        xadd(&node_count, -1);
#endif
    } 
	
    free(ll);
	
#ifdef BONSAI_HASHSET_DEBUG
    bonsai_debug("node_count = %d.\n", node_count);
    bonsai_debug("retire_count = %d.\n", retire_count);

    retire_count = 0;
    node_count = 0;
    print_count = 0;
#endif
}
