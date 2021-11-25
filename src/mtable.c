#include "mtable.h"
#include "oplog.h"
#include "pnode.h"
#include "cmp.h"
#include "common.h"

#define CUCKOO_RESIZE_THREASHOLD    	0.5
#define CUCKOO_RESIZE_MULT      		2
#define CUCKOO_MAX_DEPTH        		2
#define CUCKOO_NUM_ENTRIES				32

#define HASH1(key, cuckoo) (hash(key, \
    cuckoo->seed[0].murmur, cuckoo->seed[0].mixer) & (cuckoo->size - 1));
#define HASH2(key, cuckoo) (hash(key, \
    cuckoo->seed[1].murmur, cuckoo->seed[1].mixer) & (cuckoo->size - 1));

static void get_rand_seed(struct mtable* table) {
    srand(time(0));
    int i;
    for (i = 0; i < 3; i++) {
        table->seeds[i] = ((int64_t)rand() << 32) | rand();
    }
}

struct mtable* mtable_init(struct pnode* pnode) {
    struct mtable* table;
	int i;

	table = malloc(sizeof(struct mtable));
    rwlock_init(cuckoo->lock);
    cuckoo->total_ent = CUCKOO_NUM_ENTRIES;
    cuckoo->used_ent = 0;
    
    get_rand_seed(table);

	table->pnode = pnode;
	INIT_LIST_HEAD(&table->list);

    table->e = malloc(size, sizeof(mtable_ent_t));
    
    for (i = 0; i < table->total_ent; i++) {
		table->e[i].used = 0;
		table->e[i].addr = NULL;
        rwlock_init(&table->e[i].lock);
    }
    return mtable;
}

void resize(struct mtable* table, int reseed) {
    uint32_t old_size = table->total_ent;
	int i, idx1, idx2;
	pkey_t key

    if (reseed)
        get_rand_seed(cuckoo);
    
    table->size = table->size * CUCKOO_RESIZE_MULT;
    table->max_used_size = table->size * CUCKOO_RESIZE_THREASHOLD;
    table->e = ralloc(table->e, sizeof(cuckoo_node_t) * table->size);
    
    
    for (i = old_size; i < table->size; i++) {
        table->e[i].used = 0;
        rwlock_init(&table->e[i].lock);
    }

    for (i = 0; i < old_size; i++) {
        if (table->e[i].used) {
            key = GET_KEY(table->e[i].addr);
            idx1 = HASH1(key);
            idx2 = HASH2(key);
            if (i != idx1 && i != idx2) {
                table->e[i].used = 0;
                table->size--;
                table_insert(table, key, GET_VALUE(table->e[i].addr);
            }
        }
    }
}

void go_deep(struct mtable* table, int idx, int depth) {
    pkey_t key = GET_KEY(table->e[idx].ptr);
    int idx1 = HASH1(key, table);
    int idx2 = HASH2(key, table);
    int u_idx = (idx1 == idx) ? idx2 : idx1;

    if (table->e[u_idx].used) {
        if (depth) {
            go_deep(table, u_idx, depth - 1)
            return;
        }
        else {
            resize(table, 1);
            return;
        }
    }
    table->e[u_idx].addr = table->e[idx].addr;
    table->e[u_idx].used = 1;
    table->e[idx].used = 0;
}

/* mtable_insert: insert an entry into a mtable, if existed, update. 
 * @return: key existed/not_existed
 */
int mtable_insert(struct mtable* table, pkey_t key, pval_t value) {
    int ret;

    read_lock(&table->lock);
    if (find_index(table, key) != -1) 
        return 0;

    if (table->used_size > table->max_used_size) {
        read_unlock(&table->lock);
        resize(table, 0);
        if (table->used_size > table->max_used_size) {
            write_lock(&table->lock);
        }
        else {
            read_lock(&table->lock);
        }
    }
    
    while(1) {
        int idx1, idx2;
        pkey_t e_key;
        struct oplog* op_log;

        idx1 = HASH1(key, table);
        idx2 = HASH2(key, table);
        write_lock(&table->e[idx1].lock);
        write_lock(&table->e[idx2].lock);
        
        if (table->e[idx1].used) {
            e_key = GET_KEY(table->e[idx1].addr);
            if (cmp(e_key, key) == 0) {
                ret = -EEXIST;
                goto end;
            }

            if (!table->e[idx2].used) {
                op_log = oplog_insert(key, value, table->pnode, OP_INSERT);
                table->e[idx2].addr = op_log;
                table->e[idx2].used = 1;
                table->used_size++;
                ret = 0;
            goto end;
        }
        else if (table->e[idx2].used) {
            pkey_t e_key;

            e_key = GET_KEY(table->e[idx2].addr);
            if (cmp(e_key, key) == 0) {
                exist = -EEXIST;
                goto end;
            }

            if (!table->e[idx1].used) {
                op_log = oplog_insert(key, value, table->pnode, OP_INSERT);
                table->e[idx1].addr = op_log;
                table->e[idx1].used = 1;
                table->used_size++;
                exist = 0;
                goto end;
            }
        }

        go_deep(table, idx1, CUCKOO_MAX_DEPTH);
        write_unlock(&table->e[idx1]);
        write_unlock(&table->e[idx2]);
        continue;
end:
        write_unlock(&table->e[idx1]);
        write_unlock(&table->e[idx2]);
        break;
    }

    return ret;
}

/*
 * mtable_lookup: lookup a entry in a mtable
 * @return: key existed/not_existed
 * @reulst: value if existed
 */
int mtable_lookup(struct mtable* table, pkey_t key, pkey_t* result) {
	int idx, idx1, idx2;
    pkey_t e_key;

    idx1 = HASH1(key, cuckoo);
    idx2 = HASH2(key, cuckoo);
	idx = -1;

    read_lock(&table->e[idx1].lock);
    read_lock(&table->e[idx2].lock);

    if (table->e[idx1].used && 
    cmp(key, GET_KEY(table->e[idx1].addr)) == 0) {
        idx = idx1; 
    } else if (table->e[idx2].used && 
    cmp(key, GET_KEY(table->e[idx2].addr)) == 0) {
        idx = idx2; 
    }

    if (idx == -1) {
        read_unlock(&table->e[idx1].lock);
        read_unlock(&table->e[idx2].lock);
        return 0;
    }
    
    *result = GET_VALUE(table->e[idx].addr);
    read_unlock(&table->e[idx1].lock);
    read_unlock(&table->e[idx2].lock);

    return -EEXIST;
}

/*
 * mtable_remove: remove an entry in a mtable
 * @return: key existed/not_existed
 */
int mtable_remove(struct mtable* table, pkey_t key) {
	int idx, idx1, idx2;
    pkey_t e_key;
	int ret;

    idx1 = HASH1(key, cuckoo);
    idx2 = HASH2(key, cuckoo);
	idx = -1;

    read_lock(&table->e[idx1].lock);
    read_lock(&table->e[idx2].lock);

    if (table->e[idx1].used && 
    cmp(key, GET_KEY(table->e[idx1].addr)) == 0) {
        idx = idx1; 
    }
    else if (table->e[idx2].used && 
    cmp(key, GET_KEY(table->e[idx2].addr)) == 0) {
        idx = idx2; 
    }

    if (idx == -1) {
        read_unlock(&table->e[idx1].lock);
        read_unlock(&table->e[idx2].lock);
        return -EEXIST;
    }
    
    table->e[idx].used = 0;
    read_unlock(&table->e[idx1].lock);
    read_unlock(&table->e[idx2].lock);

    ret = oplog_insert(0, 0, table->e[idx].addr, OP_REMOVE);

    return ret;
}
