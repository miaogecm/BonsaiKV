#ifndef ARCH_H
#define ARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include "common.h"

/*
 * machine-, architecture-specific information
 */
#define ____ptr_aligned __attribute__((aligned(sizeof(void *))))

#define CACHELINE_SIZE  64
#define XPLINE_SIZE     256

//#define ARCH_SUPPORT_CLWB

#define PAGE_SHIFT	12
#define PAGE_SIZE	(1UL << PAGE_SHIFT)
#define PAGE_MASK 	(~(PAGE_SIZE-1))

#define L1_CACHE_BYTES 64
#define ____cacheline_aligned __attribute__((aligned(L1_CACHE_BYTES)))

#define CACHE_LINE_PREFETCH_UNIT (2)
#define CACHE_DEFAULT_PADDING                                                  \
	((CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES) / sizeof(long))

#define ____cacheline_aligned2                                                 \
	__attribute__((aligned(CACHE_LINE_PREFETCH_UNIT * L1_CACHE_BYTES)))

#define ____page_aligned __attribute__((aligned(PAGE_SIZE)))

#ifndef __packed
#define __packed __attribute__((packed))
#endif

#ifndef __read_mostly
#define __read_mostly __attribute__((__section__(".data..read_mostly")))
#endif

#define barrier()       __asm__ __volatile__("": : :"memory")

static inline void memory_mfence() {
    __asm__ __volatile__("mfence" : : : "memory");
}

static inline void memory_lfence() {
    __asm__ __volatile__("lfence" : : : "memory");
}

static inline void memory_sfence() {
    __asm__ __volatile__("sfence" : : : "memory");
}

#define smp_rmb()       barrier()
#define smp_wmb()       barrier()
#define smp_mb()        asm volatile("lock; addl $0,-4(%%rsp)" ::: "memory", "cc")

#define cpu_relax() asm volatile("pause\n" : : : "memory")

static inline uint64_t __attribute__((__always_inline__)) read_tsc(void)
{
	uint32_t a, d;
	__asm __volatile("rdtsc" : "=a"(a), "=d"(d) : : "%rcx");
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline uint64_t __attribute__((__always_inline__)) read_tscp(void)
{
	uint32_t a, d;
	__asm __volatile("rdtscp" : "=a"(a), "=d"(d) : : "%rcx");
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

#define clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
#define clflushopt(addr)\
	asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))
#define clwb(addr)\
	asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))

static inline void cpuid(int i, unsigned int *a, unsigned int *b,
			 unsigned int *c, unsigned int *d)
{
	/* https://bit.ly/2uGziVO */
	__asm __volatile("cpuid"
			 : "=a"(*a), "=b"(*b), "=c"(*c), "=d"(*d)
			 : "a"(i), "c"(0));
}

static inline unsigned int max_cpu_freq(void)
{
	/* https://bit.ly/2EbkRZp */
	unsigned int regs[4];
	cpuid(0x16, &regs[0], &regs[1], &regs[2], &regs[3]);
	return regs[1];
}

#define cache_prefetchr_high(__ptr) __builtin_prefetch((void *)__ptr, 0, 3)

#define cache_prefetchr_mid(__ptr) __builtin_prefetch((void *)__ptr, 0, 2)

#define cache_prefetchr_low(__ptr) __builtin_prefetch((void *)__ptr, 0, 0)

#define cache_prefetchw_high(__ptr) __builtin_prefetch((void *)__ptr, 1, 3)

#define cache_prefetchw_mid(__ptr) __builtin_prefetch((void *)__ptr, 1, 2)

#define cache_prefetchw_low(__ptr) __builtin_prefetch((void *)__ptr, 1, 0)

static inline void persistent_barrier() {
#ifdef ARCH_SUPPORT_CLWB
    memory_sfence();
#else
    memory_mfence();
#endif
}

static inline void bonsai_flush(void* buf, uint32_t len, int fence) {
    uint32_t i;

    len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
    for (i = 0; i < len; i += CACHELINE_SIZE) {
#ifdef ARCH_SUPPORT_CLWB
            clwb(buf + i);
#else
            clflush(buf + i);
#endif
    }

	if (fence) {
        persistent_barrier();
    }
}

/*
 * Code taken from:
 * https://github.com/twitter-archive/bittern/blob/master/bittern-cache/src/bittern_cache_kmod/memcpy_nt.c
 */
static void memcpy_nt(void *dst, void *src, size_t len, int fence) {
	int i;
	long long t1, t2, t3, t4;
	unsigned char *from, *to;
	size_t remain = len & (CACHELINE_SIZE - 1);

	from = src;
	to = dst;
	i = len / CACHELINE_SIZE;

	for (; i > 0; i--) {
		__asm__ __volatile__("  mov (%4), %0\n"
				     "  mov 8(%4), %1\n"
				     "  mov 16(%4), %2\n"
				     "  mov 24(%4), %3\n"
				     "  movnti %0, (%5)\n"
				     "  movnti %1, 8(%5)\n"
				     "  movnti %2, 16(%5)\n"
				     "  movnti %3, 24(%5)\n"
				     "  mov 32(%4), %0\n"
				     "  mov 40(%4), %1\n"
				     "  mov 48(%4), %2\n"
				     "  mov 56(%4), %3\n"
				     "  movnti %0, 32(%5)\n"
				     "  movnti %1, 40(%5)\n"
				     "  movnti %2, 48(%5)\n"
				     "  movnti %3, 56(%5)\n"
                     : "=&r"(t1), "=&r"(t2), "=&r"(t3), "=&r"(t4)
				     : "r"(from), "r"(to)
				     : "memory");

		from += CACHELINE_SIZE;
		to += CACHELINE_SIZE;
	}

	/*
	 * Now do the tail of the block:
	 */
	if (remain) {
		memcpy(to, from, remain);
		bonsai_flush(to, remain, 0);
	}

    if (fence) {
        persistent_barrier();
    }
}

#ifdef __cplusplus
}
#endif

#endif
