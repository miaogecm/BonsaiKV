#ifndef SPINLOCK_H
#define SPINLOCK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "atomic.h"

typedef struct {
    unsigned int slock;
} spinlock_t;

#define SPIN_INIT {0}
#define SPINLOCK_UNLOCK	0

#define __SPINLOCK_UNLOCKED	{ .slock = SPINLOCK_UNLOCK }

#define DEFINE_SPINLOCK(x)	spinlock_t x = __SPINLOCK_UNLOCKED

static inline spinlock_t* spin_lock_init(spinlock_t *lock) {
    lock->slock = 0;
    return lock;
}

static inline void spin_lock(spinlock_t *lock) {
    short inc = 0x0100;
    __asm__ __volatile__ ("  lock; xaddw %w0, %1;"
                          "1:"
                          "  cmpb %h0, %b0;"
                          "  je 2f;"
                          "  rep ; nop;"
                          "  movb %1, %b0;"
                          "  jmp 1b;"
                          "2:"
    : "+Q" (inc), "+m" (lock->slock)
    :: "memory", "cc");
}

static inline void spin_unlock(spinlock_t *lock) {
    __asm__ __volatile__("lock; incb %0;" : "+m" (lock->slock) :: "memory", "cc");
}

#ifdef __cplusplus
}
#endif

#endif
