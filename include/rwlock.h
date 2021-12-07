#ifndef RWLOCK_H
#define RWLOCK_H

#ifdef __cplusplus
extern "C" {
#endif

#include "spinlock.h"

/*
 * Writer states & reader shift and bias
 */
#define	_QW_WAITING	1			/* A writer is waiting	   */
#define	_QW_LOCKED	0xff		/* A writer holds the lock */
#define	_QW_WMASK	0xff		/* Writer mask		   */
#define	_QR_SHIFT	8			/* Reader count shift	   */
#define _QR_BIAS	(1U << _QR_SHIFT)

/*
 * 8 bytes
 */
typedef struct {
    atomic_t 	cnts; // counter for readers
    spinlock_t	slock;
} rwlock_t;

#define RWLOCK_UNLOCK {			\
	.cnts = ATOMIC_INIT(0),		\
	.slock = ATOMIC_INIT(0),	\
}

#define DEFINE_RWLOCK(x)	rwlock_t x = RWLOCK_UNLOCK

static inline void rwlock_init(rwlock_t *lock) {
    atomic_set(&lock->cnts, 0);
    spin_lock_init(&lock->slock);
}

void __read_lock(rwlock_t* lock);
void __read_unlock(rwlock_t* lock);
void __write_lock(rwlock_t* lock);
void __write_unlock(rwlock_t* lock);

#define read_lock(lock)			do{ barrier(); __read_lock(lock); } while (0)
#define read_unlock(lock)		do{ barrier(); __read_unlock(lock); } while (0)
#define write_lock(lock)		do{ barrier(); __write_lock(lock); } while (0)
#define write_unlock(lock)		do{ barrier(); __write_unlock(lock); } while (0)

#ifdef __cplusplus
}
#endif

#endif
