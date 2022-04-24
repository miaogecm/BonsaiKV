#ifndef STM_DEFINE_H
#define STM_DEFINE_H

#include <setjmp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "atomic.h"
#include "arch.h"

struct stm_tx;

typedef uintptr_t stm_word_t;
//typedef intptr_t stm_word_t;

#define ALIGNED                 __attribute__((aligned(CACHELINE_SIZE)))

typedef union stm_tx_attr {
  struct {
  /* Application-specific identifier for the transaction. */
  unsigned int id : 16;
  /* Indicates whether the transaction is read-only. */
  unsigned int read_only : 1;
  /* Indicates whether the transaction should use visible reads. */
  unsigned int visible_reads : 1;
  /**
   * Indicates that the transaction should not retry execution using
   * sigsetjmp() after abort.
   */
  unsigned int no_retry : 1;
  };
  /**
   * All transaction attributes represented as one integer.
   * For convenience, allow (stm_tx_attr_t)0 cast.
   */
  int32_t attrs;
} stm_tx_attr_t;


enum {
  /**
   * Indicates that the instrumented code path must be executed.
   */
  STM_PATH_INSTRUMENTED = 0x01,
  /**
   * Indicates that the uninstrumented code path must be executed
   * (serial irrevocable mode).
   */
  STM_PATH_UNINSTRUMENTED = 0x02,
  /**
   * Abort due to explicit call from the programmer.
   */
  STM_ABORT_EXPLICIT = (1 << 5),
  /**
   * Abort and no retry due to explicit call from the programmer.
   */
  STM_ABORT_NO_RETRY = (1 << 5) | (0x01 << 8),
  /**
   * Implicit abort (high order bits indicate more detailed reason).
   */
  STM_ABORT_IMPLICIT = (1 << 6),
  /**
   * Abort upon reading a memory location being read by another
   * transaction.
   */
  STM_ABORT_RR_CONFLICT = (1 << 6) | (0x01 << 8),
  /**
   * Abort upon writing a memory location being read by another
   * transaction.
   */
  STM_ABORT_RW_CONFLICT = (1 << 6) | (0x02 << 8),
  /**
   * Abort upon reading a memory location being written by another
   * transaction.
   */
  STM_ABORT_WR_CONFLICT = (1 << 6) | (0x03 << 8),
  /**
   * Abort upon writing a memory location being written by another
   * transaction.
   */
  STM_ABORT_WW_CONFLICT = (1 << 6) | (0x04 << 8),
  /**
   * Abort upon read due to failed validation.
   */
  STM_ABORT_VAL_READ = (1 << 6) | (0x05 << 8),
  /**
   * Abort upon write due to failed validation.
   */
  STM_ABORT_VAL_WRITE = (1 << 6) | (0x06 << 8),
  /**
   * Abort upon commit due to failed validation.
   */
  STM_ABORT_VALIDATE = (1 << 6) | (0x07 << 8),
  /**
   * Abort upon deferring to an irrevocable transaction.
   */
  STM_ABORT_IRREVOCABLE = (1 << 6) | (0x09 << 8),
  /**
   * Abort due to being killed by another transaction.
   */
  STM_ABORT_KILLED = (1 << 6) | (0x0A << 8),
  /**
   * Abort due to receiving a signal.
   */
  STM_ABORT_SIGNAL = (1 << 6) | (0x0B << 8),
  /**
   * Abort due to reaching the write set size limit.
   */
  STM_ABORT_EXTEND_WS = (1 << 6) | (0x0C << 8),
  /**
   * Abort due to other reasons (internal to the protocol).
   */
  STM_ABORT_OTHER = (1 << 6) | (0x0F << 8)
};


#define JMP_BUF                        	jmp_buf
#define LONGJMP(ctx, value)            	longjmp(ctx, value)
#define SETJMP(ctx)						          setjmp(ctx)

enum {                                  /* Transaction status */
  TX_IDLE = 0,
  TX_ACTIVE = 1,                        /* Lowest bit indicates activity */
  TX_COMMITTED = (1 << 1),
  TX_ABORTED = (2 << 1),
  TX_COMMITTING = (1 << 1) | TX_ACTIVE,
  TX_ABORTING = (2 << 1) | TX_ACTIVE,
};

#define STATUS_BITS                     4
#define STATUS_MASK                     ((1 << STATUS_BITS) - 1)

#define SET_STATUS(s, v)               ((s) = (v))
#define UPDATE_STATUS(s, v)            ((s) = (v))
#define GET_STATUS(s)                  ((s))

#define IS_ACTIVE(s)                    ((GET_STATUS(s) & 0x01) == TX_ACTIVE)

/*
 * A lock is a unsigned integer of the size of a pointer.
 * The LSB is the lock bit. If it is set, this means:
 * - At least some covered memory addresses is being written.
 * - All bits of the lock apart from the lock bit form
 *   a pointer that points to the write log entry holding the new
 *   value. Multiple values covered by the same log entry and orginized
 *   in a linked list in the write log.
 * If the lock bit is not set, then:
 * - All covered memory addresses contain consistent values.
 * - All bits of the lock besides the lock bit contain a version number
 *   (timestamp).
 *   - The high order bits contain the commit time.
 *   - The low order bits contain an incarnation number (incremented
 *     upon abort while writing the covered memory addresses).
 * When visible reads are enabled, two bits are used as read and write
 * locks. A read-locked address can be read by an invisible reader.
 */

#define OWNED_BITS                     	1                   /* 1 bit */
#define WRITE_MASK                     	0x01                /* 1 bit */
#define OWNED_MASK                     	(WRITE_MASK)

#define INCARNATION_BITS                3                   /* 3 bits */
#define INCARNATION_MAX                 ((1 << INCARNATION_BITS) - 1)
#define INCARNATION_MASK                (INCARNATION_MAX << 1)
#define LOCK_BITS                       (OWNED_BITS + INCARNATION_BITS)
#define MAX_THREADS                     8192                /* Upper bound (large enough) */
#define VERSION_MAX                     ((~(stm_word_t)0 >> LOCK_BITS) - MAX_THREADS)

#define RW_SET_SIZE                    	512                /* Initial size of read/write sets */

#define LOCK_GET_OWNED(l)               (l & OWNED_MASK)
#define LOCK_GET_WRITE(l)               (l & WRITE_MASK)
#define LOCK_SET_ADDR_WRITE(a)          (a | WRITE_MASK)    /* WRITE bit set */
#define LOCK_GET_ADDR(l)                (l & ~(stm_word_t)OWNED_MASK)

#define LOCK_GET_TIMESTAMP(l)           (l >> (LOCK_BITS))
#define LOCK_SET_TIMESTAMP(t)           (t << (LOCK_BITS))
#define LOCK_GET_INCARNATION(l)         ((l & INCARNATION_MASK) >> OWNED_BITS)
#define LOCK_SET_INCARNATION(i)         (i << OWNED_BITS)   /* OWNED bit not set */
#define LOCK_UPD_INCARNATION(l, i)      ((l & ~(stm_word_t)(INCARNATION_MASK | OWNED_MASK)) | LOCK_SET_INCARNATION(i))

/*
 * We use an array of locks and hash the address to find the location of the lock.
 * We try to avoid collisions as much as possible (two addresses covered by the same lock).
 */
#define LOCK_ARRAY_LOG_SIZE            	20                  /* Size of lock array: 2^20 = 1M */
#define LOCK_SHIFT_EXTRA               	2                   /* 2 extra shift */
#define LOCK_ARRAY_SIZE                 (1 << LOCK_ARRAY_LOG_SIZE)
#define LOCK_MASK                       (LOCK_ARRAY_SIZE - 1) // 0xFFFFF
#define LOCK_SHIFT                      (((sizeof(stm_word_t) == 4) ? 2 : 3) + LOCK_SHIFT_EXTRA) // 5
#define LOCK_IDX(a)                     (((stm_word_t)(a) >> LOCK_SHIFT) & LOCK_MASK)
#define GET_LOCK(a)                    	(_tinystm->locks + LOCK_IDX(a))

/* At least twice a cache line (not required if properly aligned and padded) */
#define CLOCK                           (_tinystm->gclock[(CACHELINE_SIZE * 2) / sizeof(stm_word_t)])

#define GET_CLOCK                       (ATOMIC_LOAD_ACQ(&CLOCK))
#define FETCH_INC_CLOCK                 (ATOMIC_FETCH_INC_FULL(&CLOCK))

typedef struct r_entry {                /* Read set entry */
  stm_word_t version;                   /* Version read */
  volatile stm_word_t *lock;            /* Pointer to lock (for fast access) */
} r_entry_t;

typedef struct r_set {                  /* Read set */
  r_entry_t *entries;                   /* Array of entries */
  unsigned int nb_entries;              /* Number of entries */
  unsigned int size;                    /* Size of array */
} r_set_t;

typedef struct w_entry {                /* Write set entry */
  union {                               /* For padding... */
    struct {
      volatile stm_word_t *addr;        /* Address written */
      stm_word_t value;                 /* New (write-back) or old (write-through) value */
      stm_word_t mask;                  /* Write mask */
      stm_word_t version;               /* Version overwritten */
      volatile stm_word_t *lock;        /* Pointer to lock (for fast access) */
      union {
        struct w_entry *next;           /* WRITE_BACK_ETL || WRITE_THROUGH: Next address covered by same lock (if any) */
        stm_word_t no_drop;             /* WRITE_BACK_CTL: Should we drop lock upon abort? */
      };
    };
    char padding[CACHELINE_SIZE];       /* Padding (multiple of a cache line) */
    /* Note padding is not useful here as long as the address can be defined in the lock scheme. */
  };
} w_entry_t;

typedef struct w_set {                  /* Write set */
  w_entry_t *entries;                   /* Array of entries */
  unsigned int nb_entries;              /* Number of entries */
  unsigned int size;                    /* Size of array */
  union {
    unsigned int has_writes;            /* WRITE_BACK_ETL: Has the write set any real write (vs. visible reads) */
    unsigned int nb_acquired;           /* WRITE_BACK_CTL: Number of locks acquired */
  };
} w_set_t;

typedef struct stm_tx {                 /* Transaction descriptor */
  JMP_BUF env;                          /* Environment for setjmp/longjmp */
  stm_tx_attr_t attr;                   /* Transaction attributes (user-specified) */
  volatile stm_word_t status;           /* Transaction status */
  stm_word_t start;                     /* Start timestamp */
  stm_word_t end;                       /* End timestamp (validity range) */
  r_set_t r_set;                        /* Read set */
  w_set_t w_set;                        /* Write set */
  unsigned int nesting;                 /* Nesting level */
  //struct stm_tx *next;                  /* For keeping track of all transactional threads */
} stm_tx_t;

/* This structure should be ordered by hot and cold variables */
struct stm_global {
  //volatile stm_word_t locks[LOCK_ARRAY_SIZE] ALIGNED;
  volatile stm_word_t locks[LOCK_ARRAY_SIZE/2] ALIGNED;
  volatile stm_word_t gclock[512 / sizeof(stm_word_t)] ALIGNED; // 64
  unsigned int initialized;             /* Has the library been initialized? */
  //volatile stm_word_t quiesce;          /* Prevent threads from entering transactions upon quiescence */
  //volatile stm_word_t threads_nb;       /* Number of active threads */
  //stm_tx_t *threads;                    /* Head of linked list of threads */
  //pthread_mutex_t quiesce_mutex;        /* Mutex to support quiescence */
  //pthread_cond_t quiesce_cond;          /* Condition variable to support quiescence */
  /* At least twice a cache line (256 bytes to be on the safe side) */
  char padding[CACHELINE_SIZE];
} ALIGNED;

struct index_struct;

extern void bonsai_stm_start(stm_tx_t* stx, stm_tx_attr_t attr);
extern int bonsai_stm_commit(stm_tx_t* stx);
extern void bonsai_stm_abort(stm_tx_t* stx, int reason);

extern stm_word_t bonsai_stm_load(stm_tx_t* stx, volatile stm_word_t *addr, struct index_struct* layer);
extern void bonsai_stm_store(stm_tx_t* stx, volatile stm_word_t *addr, stm_word_t value);

extern stm_tx_t* stm_init_thread(void);
extern void stm_exit_thread(stm_tx_t* tx);

#endif /* _STM_H_ */