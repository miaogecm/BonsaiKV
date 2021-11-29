#include <numa.h>

#include "mptable.h"
#include "oplog.h"
#include "pnode.h"
#include "cmp.h"

#define MPTABLE_NUM_ENTRIES	32

struct mptable* mptable_alloc(struct pnode* pnode, int num, int numa) {
    struct mptable* mptable;
    
    mptable = (struct mptable*) numa_alloc_onnode(sizeof(struct mptable), numa);

    mptable->numa = numa;
    mptable->slave = (struct mptable**) 
    numa_alloc_onnode(NUM_SOCKET * sizeof(struct mptable*), numa);
    mptable->pnode = pnode;
    
    hs_init(&mptable->hs);
    memset(mptable->slave, 0, sizeof(mptable->slave));
    LIST_HEAD_INIT(mptable->list);
    LIST_HEAD_INIT(mptable->numa_list);
    
    return mptable;
}

int mptable_insert(struct mptable* mptable, pkey_t key, pval_t val) {
	struct mptable *_table;
    struct hash_set *hs;
	struct oplog* log;
	int node, cpu = get_cpu();
	pval_t* addr, *new_addr;
	int tid;

	for (node = 0; node < NUM_SOCKET; node ++) {
		addr = hs_lookup(mptable->tables[node], tid, key);
		if (addr) {
			/* key exist */
			new_addr = hs_insert(&mptable->tables[node], tid, key);
			*new_addr = addr;
			return -EEXIST;
		} else {
			new_addr = hs_insert(&mptable->tables[node], tid, key);
    		if (new_addr) {
        		/* succeed */
        		log = oplog_insert(key, val, OP_INSERT);
        		*new_addr = &log->o_kv.val;

        		return 0;
    		}
		}
	}    
}

int mptable_remove(struct mptable* mptable, pkey_t key) {
    struct hash_set *hs;

    htable = mptable->htable;
    if (htable_remove(htable, key) == 0) {
        oplog_insert(key, val, mptable, OP_INSERT);
        return 0;
    }

    return -ENOENT;
}

int mptable_lookup(struct mptable* mptable, pkey_t key, pval_t* result) {
    int cpu, numa;
    struct mptable* local_mptable;
    struct htable* htable;
    hval_t h_res;

    cpu = get_cpu();
    numa = CPU_NUMA_NODE[cpu];

    if (mptable->slave[numa] == NULL) {
        mptable->slave[numa] = mptable_alloc(NULL, mptable_ent_num(mptable), numa);
    }
    local_mptable = mptable->slave[numa];

    htable = local_mptable->htable;
    if (htable_lookup(htable, key, &h_res) == 0) {
        *result = GET_VALUE(h_res);
        return 0;
    }

    htable = mptable->htable;
    if (htable_lookup(htable, key, &h_res) == 0) {
        hnode_t honde;
        htable_insert(local_mptable->htable, key, h_res, &honde);
        *result = GET_VALUE(h_res);
        return 0;
    }

    return -ENOEXT;
}
