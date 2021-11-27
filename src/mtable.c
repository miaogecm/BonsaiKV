#include <numa.h>

#include "mtable.h"
#include "oplog.h"
#include "pnode.h"
#include "cmp.h"
#include "common.h"

#define MTABLE_NUM_ENTRIES	32

struct mtable* mtable_alloc(struct pnode* pnode, int num, int numa) {
    struct mtable* mtable;
    
    mtable = (struct mtable*) numa_alloc_onnode(sizeof(mtable), numa);

    mtable->numa = numa;
    mtable->slave = (struct mtable**) 
    numa_alloc_onnode(NUM_SOCKET * sizeof(struct mtable*), numa);
    mtable->pnode = pnode;
    
    htable_init(mtable->htable);
    memset(mtable->slave, 0, sizeof(mtable->slave));
    LIST_HEAD_INIT(mtable->list);
    LIST_HEAD_INIT(mtable->numa_list);
    
    return mtable;
}

int mtable_insert(struct mtable* mtable, pkey_t key, pval_t val) {
    struct htable* htable;
    hnode_t* hnode;
    int flag;
    struct oplog* op_log;

    htable = mtable->htable;
    flag = htable_insert(htable, key, NULL, hnode);
    if (flag == 0) {
        /*succeed*/
        op_log = oplog_insert(key, val, mtable, OP_INSERT);
        hnode->val = op_log;

        return 0;
    }
    
    return -EEXIST;
}

int mtable_remove(struct mtable* mtable, pkey_t key) {
    struct htable* htable;

    htable = mtable->htable;
    if (htable_remove(htable, key) == 0) {
        oplog_insert(key, val, mtable, OP_INSERT);
        return 0;
    }

    return -ENOENT;
}

int mtable_lookup(struct mtable* mtable, pkey_t key, pval_t* result) {
    int cpu, numa;
    struct mtable* local_mtable;
    struct htable* htable;
    hval_t h_res;

    cpu = get_cpu();
    numa = CPU_NUMA_NODE[cpu];

    if (mtable->slave[numa] == NULL) {
        mtable->slave[numa] = mtable_alloc(NULL, mtable_ent_num(mtable), numa);
    }
    local_mtable = mtable->slave[numa];

    htable = local_mtable->htable;
    if (htable_lookup(htable, key, &h_res) == 0) {
        *result = GET_VALUE(h_res);
        return 0;
    }

    htable = mtable->htable;
    if (htable_lookup(htable, key, &h_res) == 0) {
        hnode_t honde;
        htable_insert(local_mtable->htable, key, h_res, &honde);
        *result = GET_VALUE(h_res);
        return 0;
    }

    return -ENOEXT;
}

#if 0
struct mtable* mtable_init(struct pnode* pnode, int num) {
    struct mtable* table;
	int i;

	table = malloc(sizeof(struct mtable));
    table->numa_id = 0;
    table->total_ent = num;
    table->used_ent = 0;

    rwlock_init(&table->lock);
    get_rand_seed(table);
    memset(table->slave, 0, sizeof(table->slave));

	table->pnode = pnode;
	INIT_LIST_HEAD(&table->list);

    table->e = calloc(num, sizeof(mtable_ent_t));
    
    return table;
}

static inline void resize(struct mtable* table, int num, int reseed) {
    uint32_t old_num = table->total_ent;
	int i, idx1, idx2;
	pkey_t key;

    if (reseed)
        get_rand_seed(table);
    
    table->total_ent = num;
    table->e = ralloc(table->e, sizeof(table_node_t) * num);
    
    for (i = old_num; i < num; i++) {
        table->e[i].used = 0;
        rwlock_init(&table->e[i].lock);
    }

    for (i = 0; i < old_num; i++) {
        if (table->e[i].used) {
            key = GET_KEY(table->e[i].addr);
            idx1 = HASH1(key);
            idx2 = HASH2(key);
            if (i != idx1 && i != idx2) {
                table->e[i].used = 0;
                table->used_ent--;
                mtable_insert(table, key, GET_VALUE(table->e[i].addr);
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

        go_deep(table, idx1, MTABLE_MAX_DEPTH);
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

static inline int find_index(struct mtable* table, pkey_t key, int idx1, int idx2) {
    int idx = -1;
    
    if (table->e[idx1].used && 
    cmp(key, GET_KEY(table->e[idx1].addr)) == 0) {
        idx = idx1; 
    } else if (table->e[idx2].used && 
    cmp(key, GET_KEY(table->e[idx2].addr)) == 0) {
        idx = idx2; 
    }
    
    return idx;
}

/*
 * mtable_lookup: lookup a entry in a mtable
 * @return: key existed/not_existed
 * @reulst: value if existed
 */
int mtable_lookup(struct mtable* table, pkey_t key, pval_t* result) {
	int idx, idx1, idx2;
    pkey_t e_key;
    int cpu, cpu_numa;
    struct mtable* s_table;

    cpu = get_cpu();
    cpu_numa = CPU_NUMA_ID[cpu];
    idx1 = HASH1(key, s_table);
    idx2 = HASH2(key, s_table);
    idx = -1;
    
    if (cpu_numa == table->numa_id) {
        read_lock(&table->e[idx1].lock);
        read_lock(&table->e[idx2].lock);

        idx = find_index(table, key, idx1, idx2);

        if (idx == -1) {
            read_unlock(&table->e[idx1].lock);
            read_unlock(&table->e[idx2].lock);
            return 0;
        }
        
        *result = GET_VALUE(table->e[idx].addr);
        read_unlock(&table->e[idx1].lock);
        read_unlock(&table->e[idx2].lock);

        return -EEXIST;
    } else {
        s_table = table->slave[cpu_numa];
        if (s_table = NULL) {
            table->slave[cpu_numa] = mtable_init(NULL, table->total_ent);
            s_table = table->slave[cpu_numa];
        }

        read_lock(&s_table->e[idx1].lock);
        read_lock(&s_table->e[idx2].lock);

        idx = find_index(table, key, idx1, idx2);

        if (idx == -1) {
            read_lock(&table->e[idx1].lock);
            read_lock(&table->e[idx2].lock);
            if (s_table->total_ent != tbale->total_ent) {
                resize(s_table, s_table->total_ent, 0); //todo
            }
            idx = find_index(table, key, idx1, idx2);
        }

        *result = GET_VALUE(table->e[idx].addr);
        read_unlock(&table->e[idx1].lock);
        read_unlock(&table->e[idx2].lock);

        return -EEXIST;
    } 
}

/*
 * mtable_remove: remove an entry in a mtable
 * @return: key existed/not_existed
 */
int mtable_remove(struct mtable* table, pkey_t key) {
	int idx, idx1, idx2;
    pkey_t e_key;
	int ret;

    idx1 = HASH1(key, table);
    idx2 = HASH2(key, table);
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


void mtable_split(struct mtable* table, struct pnode* pnode) {
    struct mtable* new_table;
    uint8_t* slot;
    int n, i;

    write_lock(&table->lock);
    slot = pnode->slot;
    n = slot[0];
    new_table = mtable_init(pnode);

    for (i = 1; i <= n; i++) {
        pkey_t key;
        pval_t value;
        int idx, idx1, idx2;

        key = pnode->entry[slot[i]].key;
        value = pnode->entry[slot[i]].value;
        idx1 = HASH1(key, table);
        idx2 = HASH2(key, table);
        idx = -1;

        if (table->e[idx1].used && 
        cmp(key, GET_KEY(table->e[idx1].addr)) == 0) {
            idx = idx1; 
        }
        else if (table->e[idx2].used && 
        cmp(key, GET_KEY(table->e[idx2].addr)) == 0) {
            idx = idx2; 
        }

        table->e[idx].used = 0;
        
        mtable_insert(new_table, key, value);
    }
    //todo
    // insert/remove/lookup need to check max_key first
    // index_layer insert
    write_unlock(&table->lock);
}

//todo lock_seq
#endif