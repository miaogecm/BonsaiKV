#ifndef SPINLOCK_H
#define SPINLOCK_H

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

static inline int spin_trylock(spinlock_t *lock) {
 	int tmp, new;

 	asm volatile("movzwl %2, %0\n\t"
 		     "cmpb %h0,%b0\n\t"
 		     "leal 0x100(%" REG_PTR_MODE "0), %1\n\t"
 		     "jne 1f\n\t"
 		     LOCK_PREFIX "cmpxchgw %w1,%2\n\t"
 		     "1:"
		     "sete %b1\n\t"
 		     "movzbl %b1,%0\n\t"
 		     : "=&a" (tmp), "=&q" (new), "+m" (lock->slock)
 		     :
 		     : "memory", "cc");

 	return tmp;
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

#endif
