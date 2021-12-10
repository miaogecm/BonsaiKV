/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *	   	   Shenming Liu
 *
 * Safe memory reclaimation (SMR) of lock-free concurrent link list. 
 * The SMR technique uses hazard point technique, see "Hazard Pointers: Safe Memory 
 * Reclamation for Lock-Free Objects, Maged M. Michael, TDPS vol.15, no.6, pg.491-504, 2004".
 */

#include <stdlib.h>
#include <string.h>

#include "link_list.h"
#include "hp.h"
#include "arch.h"
#include "atomic.h"
#include "numa_config.h"
#include "bonsai.h"
#include "mptable.h"

void hp_scan(struct linked_list* ll, struct hp_item* hp);

static unsigned int hp_R(unsigned int R) {
    return R + 2;
}

/*
 * hp_item_setup - thread use hp_item_setup() to allocate a hp_item belongs to it.
 * @ll: a hp_item struct is used for a specified linked_list. (A thread can holds a lot of hp_items when it accesses a lot of linked_lists)
 * @tid: thread-id of the calling thread
 * return: hp_item owned by the calling thread.
 */
struct hp_item* hp_item_setup(struct linked_list* ll, int tid) {
	unsigned int hp_H, old_H;
	struct hp_item* hp;

    //accumulate ll->count_of_hp
    while (1) {
        hp_H = ll->count_of_hp;  //How many hps are there in the system.
        //every time there comes a new thread, increase HP_K hps in the system.
        old_H = cmpxchg(&ll->count_of_hp, hp_H, hp_H + HP_K);
        if (old_H == hp_H) 
            break;  //success to increment hp_list->d_count.
    }

    hp = (struct hp_item*) malloc(sizeof(struct hp_item));
    memset(hp, 0, sizeof(struct hp_item));
    hp->d_list = (struct hp_rnode*) malloc(sizeof(struct hp_rnode));
    hp->d_list->next = NULL;
    ll->HP[tid] = hp;

    return hp;
}

/*
 * hp_setdown - Call from the ll_destroy() when the hp facility in the linked_list is no more used.
 * @ll:setdown hp of which linked list.
 */
void hp_setdown(struct linked_list* ll) {
	int i;

    //walk through the hp_list and free all the hp_items.
    for (i = 1; i < MAX_NUM_THREADS; i++) {
        hp_retire_hp_item(ll, i);
    }
}

void hp_save_addr(struct hp_item* hp, int index, hp_t hp_addr) {
    if (index == 0) {
        hp->hp0 = hp_addr;
    } else {
        hp->hp1 = hp_addr;
    }
}


void hp_clear_addr(struct hp_item* hp, int index) {
    if (index == 0) {
        hp->hp0 = 0;	
    } else {
        hp->hp1 = 0;
    }
}

hp_t hp_get_addr(struct hp_item* hp, int index) {
    if (index == 0) {
        return hp->hp0;
    } else {
        return hp->hp1;
    }
}

void hp_clear_all_addr(struct hp_item* hp) {
    hp->hp0 = 0;
    hp->hp1 = 0;
}

void hp_retire_node(struct linked_list* ll, struct hp_item* hp, hp_t hp_addr) {
	struct hp_rnode *rnode;

    rnode = (struct hp_rnode*) malloc(sizeof(struct hp_rnode));
    rnode->address = hp_addr;

    //push the rnode into the hp->d_list. there's no contention between threads.
    rnode->next = hp->d_list->next;
    hp->d_list->next = rnode;

    hp->d_count++;

	if (hp->d_count >= (int)hp_R(ll->count_of_hp)) {
		hp_scan(ll, hp);
	}
}

/* 
 * hp_retire_hp_item - when a thread exits, it ought to call hp_retire_hp_item() to retire the hp_item.
 */
void hp_retire_hp_item(struct linked_list* ll, int tid) {
    struct hp_item* hp = ll->HP[tid];
	struct hp_rnode *rnode, *old_rnode;
    
	if (hp == NULL) 
		return;

    ll->HP[tid] = NULL;
    rnode = hp->d_list;
    rnode = rnode->next;
    while (rnode) {
        free((void *)rnode->address);  //free the linked_list node.
        old_rnode = rnode;
        rnode = rnode->next;
        free(old_rnode);
        hp->d_count--;
    }

    free(hp->d_list);
    free((void *)hp);
}


/*
 * hp_scan
 */
void hp_scan(struct linked_list* ll, struct hp_item* hp) {
    unsigned int plist_len = HP_K * MAX_NUM_THREADS;
    unsigned int plist_count = 0;
	struct hp_rnode *new_d_list, *__rnode__;
	struct hp_item* __item__;
	unsigned int pi;
	int i, new_d_count, node_count;
	hp_t* plist, target_hp, hp0, hp1;

	plist = (hp_t *) malloc(plist_len * sizeof(hp_t));

    for (i = 0; i < MAX_NUM_THREADS; i++) {
		__item__ = ll->HP[i];
       	if (__item__ == NULL) 
	    	continue;
        hp0 = ACCESS_ONCE(__item__->hp0);
        hp1 = ACCESS_ONCE(__item__->hp1);
        if (hp0 != 0)
            plist[plist_count++] = hp0;
        if (hp1 != 0)
            plist[plist_count++] = hp1;
    }

    new_d_list = (struct hp_rnode *) malloc(sizeof(struct hp_rnode));  //new head of the hp->d_list.
    new_d_list->next = 0;
    new_d_count = 0;
	
    //walk through the hp->d_list.
    __rnode__ = hp->d_list->next;  //the first rnode in the hp->d_list.
    while (__rnode__) {
        //pop the __rnode__ from the hp->d_list.
        hp->d_list->next = __rnode__->next;
            
        //search the __rnode__->address in the plist.
        target_hp = __rnode__->address;
        pi = 0;
        for(; pi < plist_count; pi++) {
            if (plist[pi] == target_hp) {
                //found! push __rnode__ into new_d_list.
                __rnode__->next = new_d_list->next;
                new_d_list->next = __rnode__;
                new_d_count++;
                break;
            }
        }
        if (pi == plist_count) {
            //doesn't find target_hp in plist, the skiplist node at target_hp can be freed right now.
            free((void *)target_hp);
            //the __rnode__ can be freed too.
            free(__rnode__);
            xadd(&node_count, -1);
        }
        __rnode__ = hp->d_list->next;
    }
    free(plist);
    
    free(hp->d_list);
    hp->d_list = new_d_list;
    hp->d_count = new_d_count;
}

void thread_clean_hp_list(struct log_layer* layer, struct thread_info* thread) {
	#if 0
	struct numa_table* table;
	struct hash_set* hs;
	segment_t* segments;
	struct bucket_list** buckets;
	int node, j;
	unsigned int i;

	printf("thread[%d] clean hp list\n", thread->t_id);
	
	spin_lock(&layer->lock);
	list_for_each_entry(table, &layer->numa_table_list, list) {
		printf("thread table %016lx\n", table);
		for (node = 0; node < NUM_SOCKET; node ++) {
			hs = &MPTABLE_NODE(table, node)->hs;
			printf("capacity %lu\n", hs->capacity);
			for (i = 0; i < MAIN_ARRAY_LEN; i ++) {
				segments = hs->main_array[i];
				if (!segments) continue;
				printf("main_array[%i] %016lx\n", i, segments);
				buckets = (struct bucket_list**)segments;
				for (j = 0; j < SEGMENT_SIZE; j ++) {
					printf("buckets[%d] %016lx\n", j, buckets[j]);
					if (!buckets[j]) continue;
					hp_retire_hp_item(&buckets[j]->bucket_sentinel, thread->t_id);
				}
			}
		}
	}
	spin_unlock(&layer->lock);

	printf("thread[%d] finish clean hp list\n", thread->t_id);
	#endif
}
