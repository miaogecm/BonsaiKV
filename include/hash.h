#ifndef HASH_H
#define HASH_H

/* 2^31 + 2^29 - 2^25 + 2^22 - 2^19 - 2^16 + 1 */
#define GOLDEN_RATIO_PRIME_32 0x9e370001UL
/*  2^63 + 2^61 - 2^57 + 2^54 - 2^51 - 2^18 + 1 */
#define GOLDEN_RATIO_PRIME_64 0x9e37fffffffc0001UL

static inline unsigned int hash_32(unsigned int val, unsigned int bits)
{
        /* On some cpus multiply is faster, on others gcc will do shifts */
        unsigned int hash = val * GOLDEN_RATIO_PRIME_32;

        /* High bits are more random, so use them. */
        return hash >> (32 - bits);
}

static inline unsigned long hash_64(unsigned long val, unsigned int bits) {
	unsigned long hash = val;

#ifdef FAST_HASH
	hash = hash * GOLDEN_RATIO_PRIME_64;
#else
	/*  Sigh, gcc can't optimise this alone like it does for 32 bits. */
	unsigned long n = hash;
	n <<= 18;
	hash -= n;
	n <<= 33;
	hash -= n;
	n <<= 3;
	hash += n;
	n <<= 3;
	hash -= n;
	n <<= 4;
	hash += n;
	n <<= 2;
	hash += n;
#endif

	/* High bits are more random, so use them. */
	return hash >> (64 - bits);
}

#define P_HASH_BITS			17
#define p_hash(key) hash_64(key, P_HASH_BITS)

#endif
