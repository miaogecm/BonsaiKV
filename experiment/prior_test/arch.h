/*
 * Intel PMEM Performance Test
 */

#include <sched.h>

#define CACHELINE_SIZE 64

#define clflush(addr)\
	asm volatile("clflush %0" : "+m" (*(volatile char *)(addr)))
#define clflushopt(addr)\
	asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)))
#define clwb(addr)\
	asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(addr)))

static inline void mfence() {
    __asm__ __volatile__("mfence" : : : "memory");
}

static inline void lfence() {
    __asm__ __volatile__("lfence" : : : "memory");
}

static inline void sfence() {
    __asm__ __volatile__("sfence" : : : "memory");
}

static inline void smp_mb() {
    __asm__ __volatile__("lock; addl $0,-4(%%rsp)" ::: "memory", "cc");
}

static inline void bind_to_cpu(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind cpu failed\n");
    }
}