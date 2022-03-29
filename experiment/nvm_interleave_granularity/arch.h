/*
 * Intel PMEM Performance Test
 */

#ifndef NVM_PERF_ARCH_H
#define NVM_PERF_ARCH_H

#include <sched.h>
#include <stddef.h>
#include <stdint.h>

#define NUM_SOCKET          2
#define NUM_CPU_PER_SOCKET  24

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

/* assumes the length to be 4-byte aligned */
static inline void memset_nt(void *dest, uint32_t dword, size_t length)
{
    uint64_t dummy1, dummy2;
    uint64_t qword = ((uint64_t)dword << 32) | dword;

    asm volatile ("movl %%edx,%%ecx\n"
                  "andl $63,%%edx\n"
                  "shrl $6,%%ecx\n"
                  "jz 9f\n"
                  "1:      movnti %%rax,(%%rdi)\n"
                  "2:      movnti %%rax,1*8(%%rdi)\n"
                  "3:      movnti %%rax,2*8(%%rdi)\n"
                  "4:      movnti %%rax,3*8(%%rdi)\n"
                  "5:      movnti %%rax,4*8(%%rdi)\n"
                  "8:      movnti %%rax,5*8(%%rdi)\n"
                  "7:      movnti %%rax,6*8(%%rdi)\n"
                  "8:      movnti %%rax,7*8(%%rdi)\n"
                  "leaq 64(%%rdi),%%rdi\n"
                  "decl %%ecx\n"
                  "jnz 1b\n"
                  "9:     movl %%edx,%%ecx\n"
                  "andl $7,%%edx\n"
                  "shrl $3,%%ecx\n"
                  "jz 11f\n"
                  "10:     movnti %%rax,(%%rdi)\n"
                  "leaq 8(%%rdi),%%rdi\n"
                  "decl %%ecx\n"
                  "jnz 10b\n"
                  "11:     movl %%edx,%%ecx\n"
                  "shrl $2,%%ecx\n"
                  "jz 12f\n"
                  "movnti %%eax,(%%rdi)\n"
                  "12:\n"
    : "=D"(dummy1), "=d" (dummy2) : "D" (dest), "a" (qword), "d" (length) : "memory", "rcx");
}

static inline int node_to_cpu(int node, int cpu_idx) {
    return NUM_SOCKET * cpu_idx + node;
}

#define ARRAY_LEN(a)        (sizeof(a) / sizeof(*(a)))

static __always_inline unsigned long __fls(unsigned long word)
{
	asm("bsr %1,%0"
	    : "=r" (word)
	    : "rm" (word));
	return word;
}

static __always_inline int fls64(uint64_t x)
{
	if (x == 0)
		return 0;
	return __fls(x) + 1;
}

static inline __attribute__((const))
int __ilog2_u64(uint64_t n)
{
	return fls64(n) - 1;
}

#endif
