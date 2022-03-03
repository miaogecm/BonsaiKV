/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */

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

#endif
