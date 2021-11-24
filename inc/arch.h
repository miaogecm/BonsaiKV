#ifndef ARCH_H
#define ARCH_H

#define CACHELINE_SIZE      64

#define _mm_clflushopt(addr)\
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char*)(addr)))

static inline void clflush(void* buf, uint32_t len) {
    int i;
    len = len + ((unsigned long)(buf) & (CACHELINE_SIZE - 1));
    for (i = 0; i < len; i += CACHELINE_SIZE)
        _mm_clflushopt(buf + i);    
}

#endif
