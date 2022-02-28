/*
 * Copyright (c) 2014-2015 Mindaugas Rasiukevicius <rmind at netbsd org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Masstree - a trie of cache aware lock-less B+ trees.
 *
 * Based on design by Y. Mao, E. Kohler and R. Morris.  Reference:
 *
 *	http://pdos.csail.mit.edu/papers/masstree:eurosys12.pdf
 *
 * Some notes:
 *
 * - Keys are sliced into 64-bits per layer of the trie.
 * - Each layer is a B+ tree with fanout 16.
 * - Splits are only to-the-right; merges are not performed.
 *
 * Concurrency in a nutshell:
 *
 * - READERS: Version numbers with retry logic are used to prevent from
 *   capturing the intermediate state.  Think of "seqlocks".  The tree is
 *   walked by fetching the "stable" snapshots.  Such optimistic control
 *   provides high concurrency.  The logic is based on a few assumptions:
 *
 *   i) modifications must preserve consistency i.e. either be performed
 *   atomically in one go, or the node must be marked as "dirty" to fail
 *   the readers (which would spin retrying);
 *
 *   ii) the node destruction must be synchronised with the readers, e.g.
 *   by using Epoch-based reclamation or other techniques.
 *
 * - WRITERS: Fine-grained locking is used, i.e. any modifications to a
 *   node must acquire per-node spinlock (NODE_LOCKED bit) which provides
 *   mutual exclusion amongst concurrent writers.  Modifications to the
 *   node preserve consistency using a permutation stored in a 64-bit word.
 *   Otherwise, the NODE_INSERTING bit marks the node as "dirty" and fails
 *   the readers until the modification is complete.
 *
 * - SPLITS: Performed by locking the node, its sibling and its parent,
 *   as well as setting the NODE_SPLITTING bit to indicate that the node
 *   is "dirty" and that the tree shape is changing: this ensures that
 *   the readers either a) retry from the root b) use walk_leaves() to
 *   find the split leaves on the right hand side.
 *
 * - ROOT CHANGE AND LAYERING: The tree within a layer may: a) get a new
 *   root due to a split or collapse  b) be entirely collapsed and thus
 *   removed.  If the split count changes, find_leaf() will retry from
 *   the root.  The NODE_ISROOT flag is used to trigger walk_to_root() in
 *   order to find the real (new) root.  Partial collapse will set the
 *   NODE_DELETED flag which will also cause a re-try.
 *
 *   In a case of a layer deletion, the leaf of the upper layer is updated
 *   by walking the tree again from the top layer.  The NODE_DELETED flag
 *   is set on the lower layer root, thus failing all readers as well as
 *   writers operating on the layer.  This has an effect of "locking" its
 *   upper key slice.
 *
 *   Similarly, fixup of the root pointer is performed when the root just
 *   changes.  Note that the stale root pointer is not a problem because
 *   of the root walk-up which will be caused either by a split counter or
 *   NODE_ISROOT absence.
 *
 * - LOCKING AND LOCK ORDER: Nodes are locked bottom-up, but top-down
 *   across the layers.  They may also be locked left-to-right (think of
 *   counter clock-wise).  Pointers to the parent and previous nodes are
 *   protected by the lock of the nodes they are pointing to.
 */

#if defined(_KERNEL) || defined(_STANDALONE)
#include <sys/cdefs.h>
#include <sys/param.h>
#include <sys/types.h>
#else
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#endif

#include "utils.h"
#include "masstree.h"

/*
 * Version number layout: flags and two counters.
 */

#define	NODE_LOCKED		(1U << 0)	// lock (for the writers)
#define	NODE_INSERTING		(1U << 1)	// "dirty": for inserting
#define	NODE_SPLITTING		(1U << 2)	// "dirty": for splitting
#define	NODE_DELETED		(1U << 3)	// indicate node deletion
#define	NODE_ISROOT		(1U << 4)	// indicate root of B+ tree
#define	NODE_ISBORDER		(1U << 5)	// indicate border node
#define	NODE_DELAYER		(1U << 31)	// layer deletion

/*
 * Note: insert and split counter bit fields are adjacent such that
 * the inserts may overflow into the split.  That is, 7 + 18 bits in
 * total, thus making 2^25 the real overflow.
 */

#define	NODE_VINSERT		0x00001fc0	// insert counter (bits 6-13)
#define	NODE_VINSERT_SHIFT	6

#define	NODE_VSPLIT		0x7fffe000	// split counter (bits 13-31)
#define	NODE_VSPLIT_SHIFT	13

typedef struct mtree_inode mtree_inode_t;
typedef struct mtree_leaf mtree_leaf_t;

/*
 * Poor man's "polymorphism": a structure to access the version field.
 * NODE_ISBORDER determines whether it is interior or border (leaf) node.
 */
typedef struct {
	uint32_t	version;
	unsigned	_pad;
} mtree_node_t;

#define	NODE_MAX	15
#define	NODE_PIVOT	7

struct mtree_inode {
	uint32_t	version;
	uint8_t		nkeys;
	uint64_t	keyslice[NODE_MAX];
	mtree_node_t *	child[NODE_MAX + 1];
	mtree_inode_t *	parent;
	mtree_node_t *	gc_next;
};

struct mtree_leaf {
	uint32_t	version;
	uint16_t	removed;
	uint8_t		keyinfo[NODE_MAX];
	uint64_t	permutation;
	uint64_t	keyslice[NODE_MAX];
	void *		lv[NODE_MAX];
	mtree_leaf_t *	next;

	/*
	 * The following pointers are protected by the lock of the
	 * nodes they are pointing to.  Manipulated only during the
	 * creation, splits and node removal.
	 */
	mtree_leaf_t *	prev;
	mtree_inode_t *	parent;
	mtree_node_t *	gc_next;
};

/*
 * 16 four-bit fields in the 'permutation':
 * - The lower 4 bits hold the number of keys.
 * - The other bits hold a 15-element array which stores key indexes.
 * - The permutation from keyindex[0] to keyindex[nkeys - 1].
 */

#define	PERM_NKEYS(p)		((p) & 0xf)
#define	PERM_KEYIDX(p, i)	(((p) >> (((i) * 4) + 4)) & 0xf)

/*
 * Sequential permutation i.e. PERM_KEYIDX(p, i) == i.
 */
#define	PERM_SEQUENTIAL		(0xedcba98765432100ULL)

/*
 * Two upper bits of the key info store its type, the rest store the key
 * slice length (the maximum is 8).  The MTREE_LAYER flag is included in
 * KEY_LLEN() to distinguish MTREE_VALUE from MTREE_LAYER.
 *
 * Note: MTREE_NOTFOUND is just a dummy value.
 */
#define	KEY_LLEN(l)		((l) & 0x7f)
#define	KEY_TYPE(l)		((l) & 0xc0)

#define	MTREE_VALUE		0x00
#define	MTREE_LAYER		0x40
#define	MTREE_UNSTABLE		0x80
#define	MTREE_NOTFOUND		0xff

struct masstree {
	mtree_node_t *		root;
	mtree_node_t *		gc_nodes;
	const masstree_ops_t *	ops;
	mtree_leaf_t		initleaf;
};

/*
 * A few low-level helper routines and type casting wrappers.
 */

static inline uint64_t
fetch_word64(const void *key, const size_t len, unsigned *l, unsigned *slen)
{
	const bool aligned = ((uintptr_t)key & 7) == 0;
	const size_t left = len - (*l * sizeof(uint64_t));
	uint64_t skey;

	/* Optimisation: perform aligned fetch when possible. */
	if (__predict_true(aligned && left >= sizeof(uint64_t))) {
		skey = *((const uint64_t *)key + *l);
		*slen = sizeof(uint64_t);
	} else {
		skey = 0, *slen = left > 8 ? 8 : left;
		memcpy(&skey, (const uint64_t *)key + *l, *slen);
	}
	if (left > sizeof(uint64_t)) {
		/* Looking for a layer. */
		*slen |= MTREE_LAYER;
	}
	*l = *l + 1; /* next layer.. */
	return htobe64(skey);
}

static void
__masstree_free_wrapper(void *ptr, size_t size)
{
	(void)size;
	free(ptr);
}

static inline mtree_leaf_t *
cast_to_leaf(mtree_node_t *node)
{
	ASSERT((node->version & NODE_ISBORDER) != 0);
	return (mtree_leaf_t *)node;
}

static inline mtree_inode_t *
cast_to_inode(mtree_node_t *node)
{
	ASSERT((node->version & NODE_ISBORDER) == 0);
	return (mtree_inode_t *)node;
}

/*
 * Diagnostic checks to ease the debugging; they are valid only for
 * the single-threaded testing.
 */

static inline bool
validate_leaf(const mtree_leaf_t *leaf)
{
	NOSMP_ASSERT(!leaf->prev || leaf->prev->next == leaf);
	NOSMP_ASSERT(!leaf->next || leaf->next->prev == leaf);
	return true;
}

static inline bool
validate_inode(const mtree_inode_t *inode)
{
	unsigned nkeys = inode->nkeys;

	for (unsigned i = 1; i < nkeys; i++) {
		NOSMP_ASSERT(inode->keyslice[i - 1] < inode->keyslice[i]);
	}
	for (unsigned i = 0; i < nkeys + 1; i++) {
		uint32_t v = inode->child[i]->version;

		if ((v & NODE_DELETED) == 0 && (v & NODE_ISBORDER) != 0) {
			mtree_leaf_t *leaf = cast_to_leaf(inode->child[i]);
			NOSMP_ASSERT(validate_leaf(leaf));
		}
	}
	return true;
}

/*
 * The helper functions - some primitives for locking operations.
 */

/*
 * stable_version: capture a snapshot of the node version when neither
 * insertion nor split is happening (i.e. the node is not "dirty").
 * This will be used be used to check the sequence (and retry on change).
 */
static uint32_t
stable_version(mtree_node_t *node)
{
	unsigned bcount = SPINLOCK_BACKOFF_MIN;
	uint32_t v;

	v = node->version;
	while (__predict_false(v & (NODE_INSERTING | NODE_SPLITTING))) {
		SPINLOCK_BACKOFF(bcount);
		v = node->version;
	}
	atomic_thread_fence(memory_order_seq_cst);
	return v;
}

static inline bool
node_locked_p(const mtree_node_t *node)
{
	return (node->version & NODE_LOCKED) != 0;
}

static void
lock_node(mtree_node_t *node)
{
	unsigned bcount = SPINLOCK_BACKOFF_MIN;
	uint32_t v;
again:
	v = node->version;
	if (v & NODE_LOCKED) {
		SPINLOCK_BACKOFF(bcount);
		goto again;
	}
	if (!atomic_compare_exchange_weak(&node->version, v, v | NODE_LOCKED))
		goto again;

	/* XXX: Use atomic_compare_exchange_weak_explicit() instead. */
	atomic_thread_fence(memory_order_seq_cst);
}

static void
unlock_node(mtree_node_t *node)
{
	uint32_t v = node->version;

	ASSERT(node_locked_p(node));

	/*
	 * Increment the counter (either for insert or split).
	 * - Inserts can overflow into splits (since the range is small).
	 * - Clear NODE_ISROOT if split occured, it has a parent now.
	 */
	if (v & NODE_INSERTING) {
		uint32_t c = (v & NODE_VINSERT) + (1 << NODE_VINSERT_SHIFT);
		v = (v & ~NODE_VINSERT) | c;
	}
	if (v & NODE_SPLITTING) {
		uint32_t c = (v & NODE_VSPLIT) + (1 << NODE_VSPLIT_SHIFT);
		v = ((v & ~NODE_ISROOT) & ~NODE_VSPLIT) | (c & NODE_VSPLIT);
	}

	/* Release the lock and clear the operation flags. */
	v &= ~(NODE_LOCKED | NODE_INSERTING | NODE_SPLITTING);

	/* Note: store on an integer is atomic. */
	atomic_thread_fence(memory_order_seq_cst);
	node->version = v;
}

/*
 * unlock_gc_node: release the lock of the deleted node and stage it
 * for the garbage collection.
 */
static void
unlock_gc_node(masstree_t *tree, mtree_node_t *node)
{
	mtree_node_t *gclist;

	/* The node must be deleted; unlock it. */
	ASSERT((node->version & (NODE_DELETED | NODE_DELAYER)) != 0);
	unlock_node(node);

	do {
		gclist = tree->gc_nodes;
		if (node->version & NODE_ISBORDER) {
			cast_to_leaf(node)->gc_next = gclist;
		} else {
			cast_to_inode(node)->gc_next = gclist;
		}
	} while (!atomic_compare_exchange_weak(&tree->gc_nodes, gclist, node));
}

static inline mtree_node_t *
node_get_parent(mtree_node_t *node)
{
	if (node->version & NODE_ISBORDER) {
		mtree_leaf_t *leaf = cast_to_leaf(node);
		return (mtree_node_t *)leaf->parent;
	} else {
		mtree_inode_t *inode = cast_to_inode(node);
		return (mtree_node_t *)inode->parent;
	}
}

static inline mtree_node_t *
walk_to_root(mtree_node_t *node)
{
	mtree_node_t *parent;

	while ((parent = node_get_parent(node)) != NULL) {
		node = parent;
	}
	return node;
}

static inline void
node_set_parent(mtree_node_t *node, mtree_inode_t *parent)
{
	/* Note: the parent member is locked by the parent lock!. */
	ASSERT(parent == NULL || (node->version & NODE_DELETED) != 0 ||
	    node_locked_p((mtree_node_t *)parent));

	if ((node->version & NODE_ISBORDER) == 0) {
		mtree_inode_t *inode = cast_to_inode(node);
		inode->parent = parent;
	} else {
		mtree_leaf_t *leaf = cast_to_leaf(node);
		leaf->parent = parent;
	}
}

static mtree_node_t *
lock_parent_node(mtree_node_t *node)
{
	mtree_node_t *parent;
retry:
	if ((parent = node_get_parent(node)) == NULL) {
		return NULL;
	}
	lock_node(parent);
	if (__predict_false(node_get_parent(node) != parent)) {
		/* Race: unlock and try again. */
		unlock_node(parent);
		goto retry;
	}
	return parent;
}

static bool
key_geq(const mtree_leaf_t *leaf, uint64_t key, unsigned len)
{
	const uint64_t perm = leaf->permutation;
	const unsigned idx = PERM_KEYIDX(perm, 0);
	const uint64_t slice = leaf->keyslice[idx];
	//const unsigned slen = KEY_LLEN(leaf->keyinfo[idx]);
	//const bool empty = PERM_NKEYS(perm) == 0;
	(void)len;

	ASSERT((leaf->version & NODE_ISBORDER) != 0);
	//return !empty && (key > slice || (key == slice && len >= slen));
	return PERM_NKEYS(perm) != 0 && key >= slice;
}

/*
 * Primitives to manage leaf nodes of the B+ tree.
 */

static mtree_leaf_t *
leaf_create(const masstree_t *tree)
{
	const masstree_ops_t *ops = tree->ops;
	mtree_leaf_t *leaf;

	leaf = ops->alloc(sizeof(mtree_leaf_t));
	memset(leaf, 0, sizeof(mtree_leaf_t));
	leaf->version = NODE_ISBORDER;
	return leaf;
}

static inline unsigned
leaf_find_lv(const mtree_leaf_t *leaf, uint64_t key,
    unsigned kinfo, unsigned *type)
{
	const uint64_t perm = leaf->permutation;
	unsigned i, nkeys = PERM_NKEYS(perm);

	NOSMP_ASSERT(validate_leaf(leaf));

	for (i = 0; i < nkeys; i++) {
		const unsigned idx = PERM_KEYIDX(perm, i);
		const uint64_t slice = leaf->keyslice[idx];
		const unsigned sinfo = leaf->keyinfo[idx];

		if (key == slice && kinfo == KEY_LLEN(sinfo)) {
			*type = KEY_TYPE(sinfo);
			return idx;
		}
		if (key < slice)
			break;
	}
	*type = MTREE_NOTFOUND;
	return 0;
}

static unsigned
__leaf_find_lv(const mtree_leaf_t *leaf, uint64_t key,
    unsigned kinfo, unsigned *type)
{
	const uint64_t perm = leaf->permutation;
	unsigned i, nkeys = PERM_NKEYS(perm);

	NOSMP_ASSERT(validate_leaf(leaf));

	for (i = 0; i < nkeys; i++) {
		const unsigned idx = PERM_KEYIDX(perm, i);
		const uint64_t slice = leaf->keyslice[idx];
		const unsigned sinfo = leaf->keyinfo[idx];

		if (key == slice && kinfo == KEY_LLEN(sinfo)) {
			*type = KEY_TYPE(sinfo);
			return idx;
		}
		if (key < slice) {
			if (idx != 0) {
				*type = KEY_TYPE(sinfo);
				return idx - 1;
			} else {
				return __leaf_find_lv(leaf->prev, key, kinfo, type);
			}
		}
	}
	*type = KEY_TYPE(leaf->keyinfo[nkeys - 1]);
	return nkeys - 1;
}

static bool
leaf_insert_key(mtree_node_t *node, uint64_t key, unsigned kinfo, void *val)
{
	mtree_leaf_t *leaf = cast_to_leaf(node);
	const unsigned llen = KEY_LLEN(kinfo);
	const uint64_t perm = leaf->permutation;
	unsigned i, off, slot, nkeys = PERM_NKEYS(perm);
	uint64_t nperm;

	ASSERT(node_locked_p(node));

	/* If full - need a split. */
	if (nkeys == NODE_MAX) {
		return false;
	}

	/* Find the node position. */
	for (i = 0; i < nkeys; i++) {
		const unsigned idx = PERM_KEYIDX(perm, i);
		const uint64_t slice = leaf->keyslice[idx];

		if (key < slice)
			break;
		if (key == slice && llen <= KEY_LLEN(leaf->keyinfo[idx]))
			break;
	}
	off = (i * 4) + 4;

	/* Find a spare slot. */
	if (leaf->removed) {
		/*
		 * There were removals - look for an unused slot.
		 * Reset if it is past the current number of keys.
		 */
		slot = ffs(leaf->removed) - 1;
		if (slot < nkeys) {
			leaf->removed &= ~(1U << slot);
		} else {
			leaf->removed = 0;
		}

		/*
		 * Fail the readers as they might capture the new value
		 * for just-removed key.  See page 7 of the paper for
		 * the detailed description.
		 */
		node->version |= NODE_INSERTING;
		atomic_thread_fence(memory_order_seq_cst);
	} else {
		/* No removals: just pick the next slot. */
		slot = nkeys;
	}
	ASSERT(slot < NODE_MAX);

	/*
	 * Rearrange the permutation.  Make a slot at the offset (the first
	 * four bits are reserved for the number of keys).
	 */
	if (i != nkeys) {
		/*
		 * In the middle.  Just shift out the left and the right
		 * sides, increment the key count and merge all the bits
		 * in the correct positions.
		 */
		const uint64_t mask = UINT64_MAX << off;
		const uint64_t slotbits = (uint64_t)slot << off;
		nperm = ((perm + 1) & ~mask) | slotbits | (perm & mask) << 4;
	} else {
		/* At the end.  Increment the key count and add bits. */
		const uint64_t mask = 0xfULL << off;
		const uint64_t slotbits = (uint64_t)slot << off;
		nperm = ((perm + 1) & ~mask) | slotbits;
	}

	/* Set the key slice. */
	leaf->keyslice[slot] = key;
	leaf->keyinfo[slot] = kinfo;

	/* Set the value pointer.  It must become visible first. */
	leaf->lv[slot] = val;
	atomic_thread_fence(memory_order_seq_cst);

	/* Atomically store the new permutation. */
	leaf->permutation = nperm;

	return true;
}

static bool
leaf_remove_key(mtree_node_t *node, uint64_t key, unsigned len)
{
	mtree_leaf_t *leaf = cast_to_leaf(node);
	const uint64_t perm = leaf->permutation;
	unsigned i, idx, off, nkeys = PERM_NKEYS(perm);
	uint64_t nperm;

	ASSERT(nkeys > 0);
	ASSERT(node_locked_p(node));
	ASSERT(key >= leaf->keyslice[PERM_KEYIDX(perm, 0)]);
	idx = 0; /* XXXgcc */

	/* Find the position. */
	for (i = 0; i < nkeys; i++) {
		uint64_t slice;

		idx = PERM_KEYIDX(perm, i);
		slice = leaf->keyslice[idx];

		if (key < slice)
			break;
		if (key == slice && len <= KEY_LLEN(leaf->keyinfo[idx]))
			break;
	}
	off = (i * 4) + 4;

	/*
	 * Rearrange the permutation.  Essentially the same way as
	 * leaf_insert_key(), but the opposite.
	 */
	if (i != nkeys) {
		const uint64_t mask = UINT64_MAX << off;
		nperm = ((perm - 1) & ~mask) | ((perm >> 4) & mask);
	} else {
		nperm = perm - 1;
	}
	leaf->removed |= 1U << idx;
	leaf->permutation = nperm;
	atomic_thread_fence(memory_order_seq_cst);

	/* Indicate whether it was the last key. */
	return (nkeys - 1) == 0;
}

/*
 * Primitives to manage the interior nodes of the B+ tree.
 */

static mtree_inode_t *
internode_create(const masstree_t *tree)
{
	const masstree_ops_t *ops = tree->ops;
	mtree_inode_t *node;

	node = ops->alloc(sizeof(mtree_inode_t));
	memset(node, 0, sizeof(mtree_inode_t));
	return node;
}

static inline mtree_node_t *
internode_lookup(mtree_node_t *node, uint64_t key)
{
	mtree_inode_t *inode = cast_to_inode(node);
	unsigned i, nkeys = inode->nkeys;

	NOSMP_ASSERT(validate_inode(inode));

	for (i = 0; i < nkeys; i++)
		if (key < inode->keyslice[i])
			break;

	ASSERT(i < (NODE_MAX + 1));
	return inode->child[i];
}

static void
internode_insert(mtree_node_t *node, uint64_t key, mtree_node_t *child)
{
	mtree_inode_t *inode = cast_to_inode(node);
	unsigned i, nkeys = inode->nkeys;

	ASSERT(nkeys < NODE_MAX);
	ASSERT(node_locked_p(node));
	ASSERT(node_locked_p(child));
	ASSERT(node->version & (NODE_INSERTING | NODE_SPLITTING));
	NOSMP_ASSERT(validate_inode(inode));

	/* Find the position and move the right-hand side. */
	for (i = 0; i < nkeys; i++)
		if (key < inode->keyslice[i])
			break;
	if (i != nkeys) {
		const unsigned klen = (nkeys - i) * sizeof(uint64_t);
		const unsigned clen = (nkeys - i + 1) * sizeof(mtree_node_t *);
		memmove(&inode->keyslice[i + 1], &inode->keyslice[i], klen);
		memmove(&inode->child[i + 1], &inode->child[i], clen);
	}

	/* Insert the new key and the child. */
	inode->keyslice[i] = key;
	inode->child[i + 1] = child;
	node_set_parent(child, inode);
	atomic_thread_fence(memory_order_seq_cst);

	inode->nkeys++;
	NOSMP_ASSERT(validate_inode(inode));
}

static mtree_node_t *
internode_remove(mtree_node_t *node, uint64_t key)
{
	mtree_inode_t *inode = cast_to_inode(node);
	unsigned i, nkeys = inode->nkeys;

	ASSERT(nkeys > 0);
	ASSERT(node_locked_p(node));
	ASSERT(node->version & NODE_INSERTING);
	NOSMP_ASSERT(validate_inode(inode));

	/*
	 * Removing the last key - determine the stray leaf and
	 * return its pointer for the rotation.
	 */
	if (inode->nkeys == 1) {
		i = (key < inode->keyslice[0]);
		return inode->child[i];
	}

	/* Find the position and move the right-hand side. */
	for (i = 0; i < nkeys; i++)
		if (key < inode->keyslice[i])
			break;
	if (i != nkeys) {
		const unsigned klen = (nkeys - i - 1) * sizeof(uint64_t);
		const unsigned clen = (nkeys - i) * sizeof(mtree_node_t *);
		memmove(&inode->keyslice[i], &inode->keyslice[i + 1], klen);
		memmove(&inode->child[i], &inode->child[i + 1], clen);
	}
	inode->nkeys--;

	NOSMP_ASSERT(validate_inode(inode));
	return NULL;
}

/*
 * Split of the interior node.
 *
 * => Inserts the child into the correct node.
 * => Returns the right (new) node; the parent node is left.
 * => Returns the "middle key" for the creation of a new parent.
 */

static mtree_node_t *
split_inter_node(masstree_t *tree, mtree_node_t *parent, uint64_t ckey,
    mtree_node_t *nchild, uint64_t *midkey)
{
	mtree_inode_t *lnode = cast_to_inode(parent);
	mtree_inode_t *rnode = internode_create(tree);
	const unsigned s = NODE_PIVOT + 1, c = NODE_MAX - s;

	ASSERT(node_locked_p(parent));
	ASSERT(node_locked_p(nchild));
	ASSERT(lnode->nkeys == NODE_MAX);

	*midkey = lnode->keyslice[NODE_PIVOT];
	rnode->version = NODE_LOCKED | NODE_SPLITTING;
	rnode->parent = lnode->parent;

	/*
	 * Copy all keys after the pivot to the right-node.  The pivot
	 * will be removed and passed the upper level as a middle key.
	 */
	memcpy(rnode->keyslice, &lnode->keyslice[s], c * sizeof(uint64_t));
	for (unsigned i = 0; i <= c; i++) {
		rnode->child[i] = lnode->child[s + i];
		node_set_parent(rnode->child[i], rnode);
	}
	rnode->nkeys = c;

	/*
	 * Mark the left node as "dirty" and actually move the keys.
	 * Note the extra decrement in order to remove the pivot.
	 */
	lnode->version |= NODE_SPLITTING;
	atomic_thread_fence(memory_order_seq_cst);
	lnode->nkeys = s - 1;

	/* Insert the child into the correct parent. */
	const bool toleft = ckey < *midkey;
	mtree_node_t *pnode = (mtree_node_t *)(toleft ? lnode : rnode);
	internode_insert(pnode, ckey, nchild);

	NOSMP_ASSERT(validate_inode(lnode));
	NOSMP_ASSERT(validate_inode(rnode));
	return (mtree_node_t *)rnode;
}

/*
 * split_leaf_node: split the leaf node and insert the given key slice.
 *
 * => If necessary, performs the splits up-tree.
 * => If the root node is reached, sets a new root for the tree.
 */
static void
split_leaf_node(masstree_t *tree, mtree_node_t *node,
    uint64_t key, size_t len, void *val)
{
	mtree_leaf_t *leaf = cast_to_leaf(node), *nleaf;
	mtree_node_t *nnode, *parent;
	uint64_t perm, nkey;
	unsigned removed = 0;
	bool toright;

	ASSERT(node_locked_p(node));

	/*
	 * Create a new leaf and split the keys amongst the nodes.
	 * Attention: we split *only* to-the-right in order to ease
	 * the concurrent lookups.
	 */

	nleaf = leaf_create(tree);
	nnode = (mtree_node_t *)nleaf;
	nleaf->version |= NODE_LOCKED;

	/* Copy half of the keys. */
	perm = leaf->permutation;
	ASSERT(PERM_NKEYS(perm) == NODE_MAX);

	for (unsigned i = NODE_PIVOT; i < NODE_MAX; i++) {
		const unsigned idx = PERM_KEYIDX(perm, i);
		const unsigned nidx = i - NODE_PIVOT;

		nleaf->keyslice[nidx] = leaf->keyslice[idx];
		nleaf->keyinfo[nidx] = leaf->keyinfo[idx];
		nleaf->lv[nidx] = leaf->lv[idx];
		removed |= 1U << idx;
	}
	nleaf->version |= NODE_SPLITTING;
	nleaf->permutation = PERM_SEQUENTIAL | (NODE_MAX - NODE_PIVOT);
	atomic_thread_fence(memory_order_seq_cst);
	nkey = nleaf->keyslice[0];

	/*
	 * Notes on updating the list pointers:
	 *
	 * - Right-leaf (the new one) gets 'prev' and 'next' pointers set
	 *   since both of the nodes are locked.
	 *
	 * - The 'parent' of the right-leaf will be set upon its insertion
	 *   to the internode; only the splits use this pointer.
	 *
	 * - The left-leaf is locked and its 'next' pointer can be set
	 *   once the right-leaf is ready to be visible.
	 *
	 * - The 'prev' pointer of the leaf which is right to the
	 *   right-leaf can also be updated since the original previous
	 *   leaf is locked.
	 */
	if ((nleaf->next = leaf->next) != NULL) {
		mtree_leaf_t *next = nleaf->next;
		next->prev = nleaf;
	}
	nleaf->prev = leaf;
	nleaf->parent = leaf->parent;

	/*
	 * Mark the original leaf as "dirty" for splitting, then:
	 * - The keys are moving: reduce the nkeys and set removed bits.
	 * - Insert the new key into the correct leaf.
	 * - Update the 'next' pointer of the original leaf.
	 */
	leaf->version |= NODE_SPLITTING;
	atomic_thread_fence(memory_order_seq_cst);
	leaf->permutation -= (NODE_MAX - NODE_PIVOT);
	leaf->removed |= removed; // XXX

	toright = key_geq(nleaf, key, KEY_LLEN(len));
	leaf_insert_key(toright ? nnode : node, key, len, val);
	leaf->next = nleaf;

	NOSMP_ASSERT(validate_leaf(leaf));
	NOSMP_ASSERT(validate_leaf(nleaf));

	/*
	 * Done with the leaves - any further ascending would be on the
	 * internodes (invalidate the pointers merely for diagnostics).
	 *
	 * Both nodes locked; acquire the lock on parent node.
	 */
	leaf = nleaf = (void *)0xdeadbeef;
ascend:
	if ((parent = lock_parent_node(node)) == NULL) {
		/*
		 * We have reached the root.  Create a new interior
		 * node which will be a new root.
		 */
		mtree_inode_t *pnode = internode_create(tree);

		/* Initialise, set two children and the middle key. */
		pnode->version = NODE_LOCKED | NODE_INSERTING | NODE_ISROOT;
		pnode->keyslice[0] = nkey;
		pnode->child[0] = node;
		pnode->child[1] = nnode;
		pnode->nkeys = 1;
		atomic_thread_fence(memory_order_seq_cst);

		ASSERT(node->version & (NODE_SPLITTING | NODE_INSERTING));
		// XXX ASSERT(node->version & NODE_ISROOT);
		ASSERT(node_get_parent(node) == NULL);
		ASSERT(node_get_parent(nnode) == NULL);

		/*
		 * Long live new root!  Note: the top-root pointer is
		 * protected by the node lock.
		 */
		node_set_parent(nnode, pnode);
		node_set_parent(node, pnode);
		parent = (mtree_node_t *)pnode;

		if (tree->root == node) {
			tree->root = parent;
		}
		NOSMP_ASSERT(validate_inode(pnode));

		/* Release the locks.  Unlock will clear NODE_ISROOT. */
		unlock_node(parent);
		unlock_node(nnode);
		unlock_node(node);
		return;
	}
	ASSERT(node_locked_p(parent));
	NOSMP_ASSERT(validate_inode(cast_to_inode(parent)));

	if (__predict_false(((mtree_inode_t *)parent)->nkeys == NODE_MAX)) {
		mtree_node_t *inode;

		/*
		 * The parent node is full - split and ascend.  We can
		 * release the lock of the already existing child.
		 */
		unlock_node(node);
		inode = split_inter_node(tree, parent, nkey, nnode, &nkey);
		unlock_node(nnode);

		ASSERT(node_locked_p(parent));
		ASSERT(node_locked_p(inode));

		node = parent;
		nnode = inode;
		goto ascend;
	}

	/*
	 * The parent node is not full: mark the parent as "dirty"
	 * and then insert the new node into our parent.
	 */
	parent->version |= NODE_INSERTING;
	unlock_node(node); // memory_order_release
	internode_insert(parent, nkey, nnode);

	ASSERT(node_get_parent(nnode) == parent);
	ASSERT(node_get_parent(node) == parent);
	unlock_node(nnode);
	unlock_node(parent);
}

/*
 * collapse_nodes: collapse the intermediate nodes and indicate whether
 * the the upper layer needs cleanup/fixup (true) or not (false).
 */
static bool
collapse_nodes(masstree_t *tree, mtree_node_t *node, uint64_t key)
{
	mtree_node_t *parent, *child = NULL;
	bool toproot;

	ASSERT(node->version & NODE_DELETED);
	ASSERT(tree->root != node);

	/*
	 * Lock the parent.  If there is no parent, then the leaf is the
	 * root of a layer (but not the top layer).  Set the layer deletion
	 * flag and indicate that the upper layer needs a cleanup.
	 */
	if ((parent = lock_parent_node(node)) == NULL) {
		//ASSERT(node->version & NODE_ISROOT);
		node->version = (node->version & ~NODE_DELETED) | NODE_DELAYER;
		unlock_gc_node(tree, node);
		return true;
	}
	unlock_gc_node(tree, node);

	/* Fail the readers by pretending the insertion. */
	ASSERT((parent->version & NODE_DELETED) == 0);
	parent->version |= NODE_INSERTING;
	atomic_thread_fence(memory_order_seq_cst);

	/* Remove the key from the parent node. */
	if ((child = internode_remove(parent, key)) == NULL) {
		/* Done (no further collapsing). */
		unlock_node(parent);
		return false;
	}
	ASSERT(child != node);

	/*
	 * It was the last key, therefore rotate the tree: delete the
	 * internode and assign its child to the new parent.
	 */
	parent->version |= NODE_DELETED;
	node = parent;

	if ((parent = lock_parent_node(node)) != NULL) {
		mtree_inode_t *pnode = cast_to_inode(parent);
		unsigned i;

		NOSMP_ASSERT(validate_inode(pnode));

		/* Assign the child, set its parent pointer. */
		for (i = 0; i < pnode->nkeys; i++)
			if (key < pnode->keyslice[i])
				break;
		ASSERT(pnode->child[i] == node);
		pnode->child[i] = child;
		node_set_parent(child, pnode);
		unlock_gc_node(tree, node);

		NOSMP_ASSERT(validate_inode(pnode));
		unlock_node(parent);
		return false;
	}

	/*
	 * No parent: the child must become the new root.
	 *
	 * - The deleted internode, however, is still being the root of
	 *   the layer; clear the NODE_ISROOT pointer and set the parent
	 *   pointer to child, so the readers would retry from there.
	 *
	 * - Set the child's parent pointer to NULL as its parent has
	 *   just been marked as deleted.  At this point, concurrent
	 *   split or deletion of the child itself may happen.
	 */
	//ASSERT(node->version & NODE_ISROOT);
	node->version = (node->version & ~NODE_ISROOT) | NODE_DELETED;
	node_set_parent(node, (mtree_inode_t *)child);
	toproot = (tree->root == node);
	if (toproot) {
		tree->root = child;
	}
	atomic_thread_fence(memory_order_seq_cst);
	node_set_parent(child, NULL);
	unlock_gc_node(tree, node);

	/* Indicate that the upper layer needs a clean up. */
	return !toproot;
}

/*
 * delete_leaf_node: remove the leaf and add it for G/C, if necessary
 * triggering the layer collapse.
 *
 * => Return true if the upper layer needs a cleanup.
 */
static inline bool
delete_leaf_node(masstree_t *tree, mtree_node_t *node, uint64_t key)
{
	mtree_leaf_t *leaf = cast_to_leaf(node);
	mtree_node_t *prev, *next;

	ASSERT(node_locked_p(node));
	ASSERT((node->version & (NODE_INSERTING | NODE_SPLITTING)) == 0);

	NOSMP_ASSERT(validate_leaf(leaf));
	NOSMP_ASSERT(!leaf->prev || validate_leaf(leaf->prev));
	NOSMP_ASSERT(!leaf->next || validate_leaf(leaf->next));
	NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

	/*
	 * If this is the top level leaf, then we merely keep it empty.
	 */
	if (tree->root == node) {
		ASSERT(node_get_parent(node) == NULL);
		//ASSERT(node->version & NODE_ISROOT);
		unlock_node(node);
		return false;
	}

	/*
	 * Unlink the leaf from the doubly-linked list.
	 *
	 * First, we must lock the next leaf.  Then, since the node is
	 * empty, mark as deleted.  Any readers will fail and retry from
	 * the top at this point.
	 */
	while ((next = (mtree_node_t *)leaf->next) != NULL) {
		lock_node(next);
		if ((next->version & NODE_DELETED) == 0) {
			break;
		}
		/* Race: our 'next' pointer should be updated. */
		unlock_node(next);
	}
	node->version |= NODE_DELETED;
	atomic_thread_fence(memory_order_seq_cst);

	/*
	 * Get a stable version of the previous node and attempt to
	 * perform CAS on its 'next' pointer.  If the CAS failed or
	 * the version has changed - retry.
	 */
	while ((prev = (mtree_node_t *)leaf->prev) != NULL) {
		mtree_leaf_t *prevl = cast_to_leaf(prev);
		uint32_t v;
		bool ok;

		v = stable_version(prev);
		ok = prevl->next == (mtree_leaf_t *)next ||
		    atomic_compare_exchange_weak(&prevl->next, node, next);
		if (ok && (prev->version ^ v) <= NODE_LOCKED) {
			break;
		}
	}
	if (next) {
		mtree_leaf_t *nextl = cast_to_leaf(next);
		nextl->prev = leaf->prev;
		unlock_node(next);
	}

	NOSMP_ASSERT(!leaf->prev || validate_leaf(leaf->prev));
	NOSMP_ASSERT(!leaf->next || validate_leaf(leaf->next));

	/*
	 * Collapse the intermediate nodes (note: releases the leaf lock).
	 * This might tell the caller to clean up the upper layer.
	 */
	return collapse_nodes(tree, node, key);
}

/*
 * find_leaf: given the partial key, traverse the tree to find a leaf.
 *
 * => The traversing is done without acquiring any locks (READER).
 * => The closest matching leaf and its stable version are returned.
 */
static mtree_leaf_t *
find_leaf(mtree_node_t *root, uint64_t key, uint32_t *rv)
{
	mtree_node_t *node;
	uint32_t v;
retry:
	node = root;
	v = stable_version(node);

	/* Handle stale roots which can occur due to splits. */
	if (__predict_false((v & NODE_ISROOT) == 0)) {
		root = node = walk_to_root(node);
		v = stable_version(node);
	}

	/*
	 * Traverse the tree validating the captured child pointers on
	 * every step ("hand-over-hand validation", see p. 7 of the paper).
	 */
	while ((v & NODE_ISBORDER) == 0) {
		mtree_node_t *cnode;
		uint32_t cv, nv;

		/* Fetch the child node and get its state. */
		cnode = internode_lookup(node, key);
		cv = stable_version(cnode); // memory_order_acquire

		/*
		 * Check that the version has not changed.  Somebody may
		 * hold a lock, but we can proceed as long as the node is
		 * not marked as "dirty".
		 */
		if (__predict_true((node->version ^ v) <= NODE_LOCKED)) {
			/* Good - keep descending. */
			node = cnode;
			v = cv;
			continue;
		}

		/*
		 * If a split occurred, then the hierarchy might have
		 * been disrupted and we have to retry from the root.
		 */
		nv = stable_version(node);
		if (__predict_false((nv & NODE_VSPLIT) != (v & NODE_VSPLIT)))
			goto retry;

		/* The node was modified - retry. */
		v = nv;
	}
	NOSMP_ASSERT(validate_leaf(cast_to_leaf(node)));

	*rv = v;
	return cast_to_leaf(node);
}

static mtree_leaf_t *
walk_leaves(mtree_leaf_t *leaf, uint64_t skey, unsigned slen, uint32_t *vp)
{
	mtree_leaf_t *next;
	uint32_t v = *vp;

	/*
	 * Walk the leaves (i.e. iterate from left to right until we
	 * find the matching one) because of a version change.  This
	 * logic relies on a key invariant of the Masstree that the
	 * nodes split *only* to-the-right, therefore such iteration
	 * is reliable.
	 *
	 * Note: we check the current leaf first.
	 */
	v = stable_version((mtree_node_t *)leaf);
	next = leaf->next;

	/* Compare with the lowest key of the next leaf. */
	while ((v & NODE_DELETED) == 0 && next && key_geq(next, skey, slen)) {
		v = stable_version((mtree_node_t *)next);
		leaf = next, next = leaf->next;
	}
	*vp = v;

	/*
	 * At this point we either found our border leaf and have its
	 * stable version or hit a deleted leaf.
	 */
	return leaf;
}

static mtree_leaf_t *
find_leaf_locked(mtree_node_t *root, uint64_t skey, unsigned slen)
{
	mtree_leaf_t *leaf;
	uint32_t v, nvc;

	/*
	 * Perform the same lookup logic as in masstree_get(), but lock
	 * the leaf once found and just re-lock if walking the leaves.
	 */
retry:
	leaf = find_leaf(root, skey, &v);
forward:
	if (__predict_false(v & NODE_DELAYER)) {
		/* Tell the caller to re-try from the top root. */
		return NULL;
	}
	if (__predict_false(v & NODE_DELETED)) {
		goto retry;
	}

	/*
	 * Lock!  Check the split counter and re-check the delete flag.
	 * Note that lock_node() issues a read memory barrier for us.
	 */
	lock_node((mtree_node_t *)leaf); // memory_order_release
	nvc = leaf->version & (NODE_VSPLIT | NODE_DELETED);
	if (__predict_false(nvc != (v & NODE_VSPLIT))) {
		unlock_node((mtree_node_t *)leaf);
		leaf = walk_leaves(leaf, skey, slen, &v);
		goto forward;
	}
	return leaf;
}

/*
 * masstree_get: fetch a value given the key.
 */
void *
masstree_get(masstree_t *tree, const void *key, size_t len)
{
	mtree_node_t *root = tree->root;
	unsigned l = 0, slen, idx, type;
	mtree_leaf_t *leaf;
	uint64_t skey;
	uint32_t v;
	void *lv;
advance:
	/*
	 * Fetch a slice (64-bit word), iterating layers.  Note: sets
	 * the MTREE_LAYER flag on slice-length if looking for a later.
	 */
	skey = fetch_word64(key, len, &l, &slen);
retry:
	/* Find the leaf given the slice-key. */
	leaf = find_leaf(root, skey, &v);
forward:
	if (__predict_false(v & NODE_DELETED)) {
		/* Collided with deletion - try again from the root. */
		goto retry;
	}

	/* Fetch the value (or pointer to the next layer). */
	idx = __leaf_find_lv(leaf, skey, slen, &type);

	lv = leaf->lv[idx];
	atomic_thread_fence(memory_order_seq_cst);

	/* Check that the version has not changed. */
	if (__predict_false((leaf->version ^ v) > NODE_LOCKED)) {
		leaf = walk_leaves(leaf, skey, slen, &v);
		goto forward;
	}

	if (__predict_true(type == MTREE_VALUE)) {
		ASSERT((slen & MTREE_LAYER) == 0);
		return lv;
	}
	if (__predict_true(type == MTREE_LAYER)) {
		/* Advance the key and move to the next layer. */
		ASSERT((slen & MTREE_LAYER) != 0);
		root = lv;
		goto advance;
	}
	if (__predict_true(type == MTREE_NOTFOUND)) {
		assert(0);
	}

#if 0
	if (__predict_false((type & ~MTREE_LAYER) == MTREE_UNSTABLE)) {
		/*
		 * The value is about to become MTREE_LAYER, unless remove
		 * races and wins, therefore we have to re-check the key.
		 */
		goto forward;
	}
#endif
	return NULL;
}

/*
 * masstree_put: store a value given the key.
 *
 * => Returns true if the new entry was created.
 * => Returns false if the existing entry was modified.
 */
bool
masstree_put(masstree_t *tree, const void *key, size_t len, void *val)
{
	mtree_node_t *root = tree->root, *node;
	unsigned l = 0, slen, idx, type;
	mtree_leaf_t *leaf;
	void *sval = val;
	uint64_t skey;
advance:
	skey = fetch_word64(key, len, &l, &slen);

	/* Lookup the leaf and lock it (returns stable version). */
	leaf = find_leaf_locked(root, skey, slen);
	if (__predict_false(leaf == NULL)) {
		root = tree->root, l = 0;
		goto advance;
	}
	idx = leaf_find_lv(leaf, skey, slen, &type);
	node = (mtree_node_t *)leaf;
	NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

	if (type == MTREE_VALUE) {
		/* The key was found: store a new value. */
		leaf->lv[idx] = val;
		unlock_node(node);
		return false;
	}
	if (type == MTREE_LAYER) {
		/*
		 * Continue to the next layer.  Fixup the pointer to
		 * point to the real root, if necessary.
		 */
		root = leaf->lv[idx];
		if ((root->version & NODE_ISROOT) == 0) {
			root = walk_to_root(root);
			leaf->lv[idx] = root;
		}
		unlock_node(node);
		goto advance;
	}

	/* Note: cannot be MTREE_UNSTABLE as we acquired the lock. */
	ASSERT(type == MTREE_NOTFOUND);
newlayer:
	ASSERT(node_locked_p(node));

	/* Create a new layer. */
	if (slen & MTREE_LAYER) {
		mtree_leaf_t *nlayer;

		nlayer = leaf_create(tree);
		nlayer->version |= NODE_LOCKED | NODE_INSERTING | NODE_ISROOT;
		atomic_thread_fence(memory_order_seq_cst);
		root = sval = nlayer;
	}

	/* The key was not found: insert it. */
	if (!leaf_insert_key(node, skey, slen, sval)) {
		/* The node is full: perform the split processing. */
		split_leaf_node(tree, node, skey, slen, sval);
	} else {
		unlock_node(node);
	}

	if (slen & MTREE_LAYER) {
		/* Advance the key and jump into the next layer. */
		skey = fetch_word64(key, len, &l, &slen);
		sval = val, node = root;
		goto newlayer;
	}
	return true;
}

/*
 * masstree_del: remove they entry given the key.
 *
 * => Returns true if the key was removed; false if not found.
 */
bool
masstree_del(masstree_t *tree, const void *key, size_t len)
{
	mtree_node_t *root = tree->root, *node;
	unsigned l = 0, cleanup = 0, slen, idx, type;
	mtree_leaf_t *leaf;
	uint64_t skey;
advance:
	skey = fetch_word64(key, len, &l, &slen);

	/* Lookup the leaf and lock it (returns stable version). */
	leaf = find_leaf_locked(root, skey, slen);
	if (__predict_false(leaf == NULL)) {
		root = tree->root, l = 0;
		goto advance;
	}
	idx = leaf_find_lv(leaf, skey, slen, &type);
	node = (mtree_node_t *)leaf;
	NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

	if (type == MTREE_VALUE) {
delayer:
		ASSERT((slen & MTREE_LAYER) == 0 || cleanup);

		/* The key was found: delete it. */
		if (!leaf_remove_key(node, skey, slen)) {
			unlock_node(node);
			return true;
		}
		NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

		/* It was the last key: deleting the whole leaf. */
		if (!delete_leaf_node(tree, node, skey)) {
			return true;
		}

		/*
		 * Indicate some layer clean up.  Reset root and start
		 * from the first layer.
		 */
		cleanup = l - 1;
		root = tree->root, l = 0;
		goto advance;
	}

	if (type == MTREE_LAYER) {
		/* Continue to the next layer. */
		ASSERT((slen & MTREE_LAYER) != 0);

		/*
		 * Check if it points to the real root; if not, walk up
		 * to the real root and reset our pointer.
		 */
		root = leaf->lv[idx];
		if ((root->version & NODE_ISROOT) == 0) {
			root = leaf->lv[idx] = walk_to_root(root);
		}

		/*
		 * If we re-traversed to perform a clean-up, then check
		 * whether the layer was deleted and potentially remove
		 * the key.  Otherwise, it was a root fixup which we or
		 * a racing thread must have performed.  Just return.
		 */
		if (l == cleanup) {
			ASSERT(cleanup != 0);

			if (root->version & NODE_DELAYER) {
				/* Delete the layer key. */
				slen = (slen & ~MTREE_VALUE) | MTREE_LAYER;
				goto delayer;
			}
#if 0
			lock_node(root);
			if (node_get_parent(root) == NULL) {
				root->version |= NODE_ISROOT;
			}
			unlock_node(root);
#endif
			return true;
		}
		unlock_node(node);
		goto advance;
	}

	/*
	 * Not found: nothing to do, just unlock and return.
	 * Note: cannot be MTREE_UNSTABLE as we acquired the lock.
	 */
	ASSERT(type == MTREE_NOTFOUND);
	unlock_node(node);
	return false;
}

void *
masstree_gc_prepare(masstree_t *tree)
{
	mtree_node_t *gc_nodes = NULL;

	do {
		gc_nodes = tree->gc_nodes;
	} while (!atomic_compare_exchange_weak(&tree->gc_nodes, gc_nodes, NULL));

	return gc_nodes;
}

/*
 * masstree_gc: destroy all the garbage-collected nodes.
 */
void
masstree_gc(masstree_t *tree, void *gc)
{
	const masstree_ops_t *ops = tree->ops;
	mtree_node_t *node = gc, *next;

	while (node) {
		const uint32_t v = node->version;

		ASSERT((v & (NODE_DELETED | NODE_DELAYER)) != 0);

		if (v & NODE_ISBORDER) {
			next = cast_to_leaf(node)->gc_next;
		} else {
			next = cast_to_inode(node)->gc_next;
		}
		if (node != (mtree_node_t *)&tree->initleaf) {
			ops->free(node, (v & NODE_ISBORDER) ?
			    sizeof(mtree_leaf_t) : sizeof(mtree_inode_t));
		}
		node = next;
	}
}

masstree_t *
masstree_create(const masstree_ops_t *ops)
{
	masstree_t *tree;
	mtree_node_t *root;

	if (ops == NULL) {
		static const masstree_ops_t default_ops = {
			.alloc = malloc,
			.free = __masstree_free_wrapper
		};
		ops = &default_ops;
	}
	tree = ops->alloc(sizeof(masstree_t));
	memset(tree, 0, sizeof(masstree_t));
	tree->ops = ops;

	root = (mtree_node_t *)&tree->initleaf;
	root->version = NODE_ISROOT | NODE_ISBORDER;
	tree->root = root;

	atomic_thread_fence(memory_order_seq_cst);
	return tree;
}

void
masstree_destroy(masstree_t *tree)
{
	const masstree_ops_t *ops = tree->ops;
	mtree_leaf_t *root = cast_to_leaf(tree->root);

	/* Diagnostics: check for non-empty tree. */
	root = cast_to_leaf(tree->root);
	ASSERT(PERM_NKEYS(root->permutation) == 0);
	ASSERT(tree->gc_nodes == NULL);

	/* Finally, free Masstree. */
	// if (&tree->initleaf != root) {
	// 	ops->free(root, sizeof(mtree_leaf_t));
	// }
	// ops->free(tree, sizeof(masstree_t));
}

size_t
masstree_maxheight(void)
{
	/*
	 * The maximum height of the B-tree is:
	 *
	 *	h <= log_d((n + 1) / 2)
	 *
	 * The order of Masstree is d = 7.5 (since the fanout is 16).
	 * The number of slices has an upper bound of 2^64 and the number
	 * of keys per slice has an upper bound of 9 (eight possible values
	 * plus a value for the next layer; note that the zero length keys
	 * are not stored).  Therefore, n = 9 * 2^64 and:
	 *
	 *	h <= log7.5(((9 * 2^64) + 1) / 2) <= ~23
	 */
	return 23;
}
