/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#include "rwlock.h"

#ifndef likely
#define likely(x)               __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x)				__builtin_expect(!!(x), 0)
#endif

#ifndef cpu_relax
#define cpu_relax() asm volatile("pause\n" : : : "memory")
#endif

/**
 * rspin_until_writer_unlock - inc reader count & spin until writer is gone
 * @lock  : Pointer to queue rwlock structure
 * @writer: Current queue rwlock writer status byte
 */
static inline void rspin_until_writer_unlock(rwlock_t *lock, unsigned int cnts)
{
    while ((cnts & _QW_WMASK) == _QW_LOCKED) {
        cpu_relax();
        memory_lfence();
        cnts = atomic_read(&lock->cnts);
    }
}

static void __read_lock_slowpath(rwlock_t *lock) {
    unsigned int cnts;

    /* slow path */
    atomic_sub(_QR_BIAS, &lock->cnts);

    /* Put the reader into the wait queue */
    spin_lock(&lock->slock);

    while (atomic_read(&lock->cnts) & _QW_WMASK)
        cpu_relax();

    cnts = atomic_add_return(_QR_BIAS, &lock->cnts) - _QR_BIAS;
    rspin_until_writer_unlock(lock, cnts);

    /* Signal the next one in queue to become queue head */
    spin_unlock(&lock->slock);
}

/**
 * __read_lock - acquire read lock of a queue rwlock
 * @lock: Pointer to queue rwlock structure
 */
void __read_lock(rwlock_t* lock) {
    unsigned int cnts;

    /* fast path */
    cnts = atomic_add_return(_QR_BIAS, &lock->cnts);
    if (likely(!(cnts & _QW_WMASK)))
        return;

    __read_lock_slowpath(lock);
}

/**
 * __read_unlock - release read lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
void __read_unlock(rwlock_t* lock) {
    memory_lfence();
    /* Atomically decrement the reader count */
    atomic_sub(_QR_BIAS, &lock->cnts);
}

static void __write_lock_slowpath(rwlock_t* lock) {
    int cnts;

    /* Put the writer into the wait queue */
    spin_lock(&lock->slock);

    /* Try to acquire the lock directly if no reader is present */
    if (!atomic_read(&lock->cnts) &&
        (atomic_cmpxchg(&lock->cnts, 0, _QW_LOCKED) == 0))
        goto unlock;

    /*
     * Set the waiting flag to notify readers that a writer is pending,
     * or wait for a previous writer to go away.
     */
    for (;;) {
        cnts = atomic_read(&lock->cnts);
        if (!(cnts & _QW_WMASK) &&
            (atomic_cmpxchg(&lock->cnts, cnts, cnts | _QW_WAITING) == cnts))
            break;

        cpu_relax();
    }

    /* When no more readers, set the lock flag */
    for (;;) {
        cnts = atomic_read(&lock->cnts);
        if ((cnts == _QW_WAITING) &&
            (atomic_cmpxchg(&lock->cnts, _QW_WAITING, _QW_LOCKED) == _QW_WAITING))
            break;

        cpu_relax();
    }

    unlock:
    spin_unlock(&lock->slock);
}

/**
 * __write_lock - acquire write lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
void __write_lock(rwlock_t* lock) {
    /* fast path */
    if (atomic_cmpxchg(&lock->cnts, 0, _QW_LOCKED) == 0)
        return;

    /* slow path */
    __write_lock_slowpath(lock);
}

/**
 * __write_unlock - release write lock of a queue rwlock
 * @lock : Pointer to queue rwlock structure
 */
void __write_unlock(rwlock_t* lock) {
    memory_sfence();
    atomic_sub(_QW_LOCKED, &lock->cnts);
}