/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

#include <string.h>

#include "bonsai.h"
#include "nab.h"

#ifdef LONG_KEY

pkey_t alloc_nvkey(pkey_t vkey) {
	struct data_layer* layer = &(bonsai->d_layer);
	PMEMobjpool* pop = layer->region[0].pop;
	struct long_key __node(my) *root;
    size_t k_len = pkey_len(vkey);
	TOID(struct long_key) toid;

    assert(!pkey_is_nv(vkey));

	POBJ_ALLOC(pop, &toid, struct long_key, k_len, NULL, NULL);
	root = nab_my_ptr(pmemobj_direct(toid.oid));

	pmemobj_memcpy_persist(pop, root->key, pkey_addr(vkey), k_len);

    nab_init_region(root->key, k_len, 1);

    return pkey_generate_nv(toid.oid.off, k_len);
}

void free_nvkey(pkey_t nvkey) {
	struct data_layer* layer = &(bonsai->d_layer);
	PMEMoid oid;
	void *addr;

    assert(pkey_is_nv(nvkey));

	addr = (void *) layer->region[0].start + pkey_off(nvkey);
	oid = pmemobj_oid(addr);

	POBJ_FREE(&oid);
}

static inline size_t fetch_chunk(char *chunk, const void *key, size_t len, int nv, int pulled) {
    if (!nv || pulled) {
        if (len < NAB_BLK_SIZE) {
            memcpy(chunk, key, len);
            chunk[len] = '\0';
        } else {
            memcpy(chunk, key, NAB_BLK_SIZE);
        }
    } else {
        __nab_load(chunk, nab_node0_ptr((void *) key), NAB_BLK_SIZE);
    }
    return min(len, (size_t) NAB_BLK_SIZE);
}

int key_cmp(pkey_t a, pkey_t b, int pulled) {
#ifndef LONG_KEY
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
#else
    char ca[NAB_BLK_SIZE], cb[NAB_BLK_SIZE];
    const void *sa, *sb;
    size_t la, lb;
    size_t fa, fb;
    uint64_t aux;
    int cmp;

    sa = resolve_key(a, &aux, &la);
    sb = resolve_key(b, &aux, &lb);

    while (la > 0 && lb > 0) {
        fa = fetch_chunk(ca, sa, la, pkey_is_nv(a), pulled);
        fb = fetch_chunk(cb, sb, lb, pkey_is_nv(b), pulled);

        if ((cmp = strncmp(ca, cb, min(fa, fb))) != 0) {
            return cmp;
        }

        la -= fa;
        lb -= fb;
    }

    return (la > 0) - (lb > 0);
#endif
}

uint8_t pkey_get_signature(pkey_t key) {
    uint8_t res;
#ifndef LONG_KEY
    res = key;
#else
    const uint8_t *curr;
    uint64_t aux;
    size_t len;

    curr = resolve_key(key, &aux, &len);

    res = 0;
    while (len--) {
        res = res * 37 + *curr++;
    }
#endif
    if (unlikely(res == 0xff)) {
        res = 0;
    }
    return res;
}

pkey_t pkey_prev(pkey_t key) {
#ifndef LONG_KEY
    return key - 1;
#else
    const void *curr;
    uint8_t *prev;
    uint64_t aux;
    size_t len;

    curr = resolve_key(key, &aux, &len);

    prev = malloc(len);
	memcpy(prev, curr, len);

    prev[len - 1]--;

	return pkey_generate_v(prev, len);
#endif
}

#endif
