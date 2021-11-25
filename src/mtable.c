#include "mtable.h"

#define CUCKOO_RESIZE_THREASHOLD    	0.5
#define CUCKOO_RESIZE_MULT      		2
#define CUCKOO_MAX_DEPTH        		2

#define HASH1(key, cuckoo) (hash(key, \
    cuckoo->seed[0].murmur, cuckoo->seed[0].mixer) & (cuckoo->size - 1));
#define HASH2(key, cuckoo) (hash(key, \
    cuckoo->seed[1].murmur, cuckoo->seed[1].mixer) & (cuckoo->size - 1));

int cuckoo_cmp(entry_key_t a, entry_key_t b) {
    if (a < b) return -1;
    if (a == b) return 0;
    reutrn 1;
}

static void get_rand_seed(cuckoo_hash_t* cuckoo) {
    srand(time(0));
    int i;
    for (i = 0; i < 3; i++) {
        cuckoo->seeds[i] = ((int64_t)rand() << 32) | rand();
    }
}

cuckoo_hash_t* cuckoo_init(uint32_t size) {
    cuckoo_hash_t* cuckoo = malloc(sizeof(cuckoo_hash_t));
    assert(cuckoo);
    rwlock_init(cuckoo->lock);
    cuckoo->size = size;
    cuckoo->used_size = 0;
    cuckoo->max_used_size = size / 2;
    get_rand_seed(cuckoo);
    cuckoo->table = calloc(size, sizeof(cuckoo_node_t));
    assert(cuckoo->table);
    for (int i = 0; i < size; i++) {
        rwlock_init(cuckoo->table[i].lock);
    }
    return cuckoo;
}

int find_index(cuckoo_hash_t* cuckoo, pkey_t key) {
    int idx1 = HASH1(key, cuckoo);
    int idx2 = HASH2(key, cuckoo);
    read_lock(cuckoo->table[idx1].lock);
    read_lock(cuckoo->table[idx2].lock);

    int idx = -1;
    if (cuckoo->table[idx1].used && 
    cuckoo_cmp(key, GET_KEY(cuckoo->table[idx1].ptr)) == 0) {
        idx = idx1; 
    }
    else if (cuckoo->table[idx2].used && 
    cuckoo_cmp(key, GET_KEY(cuckoo->table[idx2].ptr)) == 0) {
        idx = idx2; 
    }
    
    read_unlock(cuckoo->table[idx1].lock);
    read_unlock(cuckoo->table[idx2].lock);
    return idx;
}

void resize(cuckoo_hash_t* cuckoo, int reseed) {
    uint32_t old_size = cuckoo->size;
    if (reseed) {
        get_rand_seed(cuckoo);
    }
    cuckoo->size = cuckoo->size * CUCKOO_RESIZE_MULT;
    cuckoo->max_used_size = cuckoo->size * CUCKOO_MAX_SIZE_USED / 100;
    cuckoo->table = ralloc(cuckoo->table, sizeof(cuckoo_node_t) * cuckoo->size);
    assert(cuckoo->table);
    int i;
    for (i = old_size; i < cuckoo->size; i++) {
        cuckoo->table[i].used = 0;
        rwlock_init(cuckoo->table[i].lock);
    }
    for (i = 0; i < old_size; i++) {
        if (cuckoo->table[i].used) {
            entry_key_t key = GET_KEY(cuckoo->table[i].ptr);
            int idx1 = HASH1(key);
            int idx2 = HASH2(key);
            if (i != idx1 && i != idx2) {
                cuckoo->table[i].used = 0;
                cuckoo->size--;
                cuckoo_insert(cuckoo, key, GET_VALUE(cuckoo->table[i].ptr);
            }
        }
    }
}

void ge_deep(cuckoo_hash_t* cuckoo, int idx, int depth) {
    entry_key_t key = GET_KEY(cuckoo->table[idx].ptr);
    int idx1 = HASH1(key, cuckoo);
    int idx2 = HASH2(key, cuckoo);
    int u_idx = (idx1 == idx) ? idx2 : idx1;
    if (cuckoo->table[u_idx].used) {
        if (depth) {
            ge_deep(cuckoo, u_idx, depth - 1)
            return;
        }
        else {
            resize(cuckoo, 1);
            return;
        }
    }
    cuckoo->table[u_idx].ptr = cuckoo->table[idx].ptr;
    cuckoo->table[u_idx].used = 1;
    cuckoo->table[idx].used = 0;
}

int cuckoo_insert(cuckoo_hash_t* cuckoo, cuckoo_hash_t key, size_t value) {
    read_lock(cuckoo->lock);
    if (find_index(cuckoo, key) != -1) {
        return 0;
    }

    if (cuckoo->used_size > cuckoo->max_used_size) {
        read_unlock(cuckoo->lock);
        resize(cuckoo, 0);
        if (cuckoo->used_size > cuckoo->max_used_size) {
            write_lock(cuckoo->lock);
        }
        else {
            read_lock(cuckoo->lock);
        }
    }
    
    while(1) {
        int idx1 = HASH1(key, cuckoo);
        int idx2 = HASH2(key, cuckoo);
        write_lock(cuckoo->table[idx1].lock);
        write_lock(cuckoo->table[idx2].lock);
        if (!cuckoo->table[idx1].used) {
            cuckoo->table[idx1].ptr = value;
            cuckoo->table[idx1].used = 1;
            cuckoo->used_size++;
            goto end;
        }
        else if (!cuckoo->table[idx2].used) {
            cuckoo->table[idx2].ptr = value;
            cuckoo->table[idx2].used = 1;
            cuckoo->used_size++;
            goto end;
        }
        go_deep(cuckoo, idx1, CUCKOO_MAX_DEPTH);
        write_unlock(cuckoo->table[idx1]);
        write_unlock(cuckoo->table[idx2]);
        continue;
end:;
        write_unlock(cuckoo->table[idx1]);
        write_unlock(cuckoo->table[idx2]);
        break;
    }
    return 1;
}
