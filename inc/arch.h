#ifndef ARCH_H
#define ARCH_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif
#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif

/*
 * machine-, architecture-specific information
 */
#define ____ptr_aligned __attribute__((aligned(sizeof(void *))))

#define CPU_MAX_NUM     48

#define PAGE_SIZE 4096
#define CACHELINE_SIZE 64

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

static inline void __attribute__((__always_inline__)) wmb(void)
{
	__asm__ __volatile__("sfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_mb(void)
{
	__asm__ __volatile__("mfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_rmb(void)
{
	__asm__ __volatile__("lfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_wmb(void)
{
	__asm__ __volatile__("sfence" ::: "memory");
}

static inline void __attribute__((__always_inline__)) barrier(void)
{
	__asm__ __volatile__("" ::: "memory");
}

static inline void __attribute__((__always_inline__)) smp_wmb_tso(void)
{
	barrier();
}

#define cpu_relax() asm volatile("pause\n" : : : "memory")

static inline uint64_t __attribute__((__always_inline__)) read_tsc(void)
{
	uint32_t a, d;
	__asm __volatile("rdtsc" : "=a"(a), "=d"(d));
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline uint64_t __attribute__((__always_inline__)) read_tscp(void)
{
	uint32_t a, d;
	__asm __volatile("rdtscp" : "=a"(a), "=d"(d));
	return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline void clflush(volatile void *p)
{
	asm volatile("clflush (%0)" ::"r"(p));
}

static inline void clflushopt(volatile void *p)
{
	asm volatile(".byte 0x66; clflush %0" : "+m"(p));
}

static inline void clwb(volatile void *p)
{
	asm volatile(".byte 0x66; xsaveopt %0" : "+m"(p));
}

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

#define _mm_clflushopt(addr)\
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char*)(addr)))

static inline void clflush(void* buf, uint32_t len) {
    int i;
    len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
    for (i = 0; i < len; i += CACHELINE_SIZE)
        _mm_clflushopt(buf + i);    
}

#ifdef __cplusplus
}
#endif

#endif
