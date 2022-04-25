/*
 * Bonsai: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 *		     Junru Shen, gnu_emacs@hhu.edu.cn	
 *
 * STM: A software transactional memory implementation based on TinySTM.
 * 
 * STM uses LSA (Lazy Snapshot Algorithm) to achieve atomicity, consistency and isolation 
 * for concurrent executing transactions.
 * (1) STM maintains a global logical clock, which increases when a transaction commits successfully.
 * (2) STM uses invisible read and validate the read sets during read operations to ensure 
 *     the consistent view of read operations. 
 * (3) STM uses visible write and encounter-time locking to achieve mutual exclusion among write
 *     operations.
 * (4) Linearization point of read-only transaction is some time between the start and the end
 *     of transaction.
 * (5) Linearization point of update transaction is the commit time of the transaction.
 */

#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <sched.h>

#include "index_layer.h"
#include "stm.h"

/* Global variables */
static struct stm_global* _tinystm;

#define INLINE   inline __attribute__((always_inline))
#define NOINLINE __attribute__((noinline))

static NOINLINE void stm_rollback(stm_tx_t *tx, unsigned int reason);
static NOINLINE void stm_allocate_rs_entries(stm_tx_t *tx, int extend);
static NOINLINE void stm_allocate_ws_entries(stm_tx_t *tx, int extend);
static INLINE r_entry_t* stm_has_read(stm_tx_t *tx, volatile stm_word_t *lock);

#define PRINT_DEBUG

static INLINE void*
malloc_aligned(size_t size)
{
  void *memptr;
  if (unlikely(posix_memalign(&memptr, CACHELINE_SIZE, size)))
    memptr = NULL;
  if (unlikely(memptr == NULL)) {
    fprintf(stderr, "Error allocating aligned memory\n");
    exit(1);
  }
  return memptr;
}

/*
 * stm_wbetl_validate - validate all read entries of @tx,
 * if the entry is locked, check the lock owner, otherwise
 * check the current lock timestamp and entry version.
 */
static INLINE int stm_wbetl_validate(stm_tx_t *tx)
{
  r_entry_t *r;
  int i;
  stm_word_t l;

  PRINT_DEBUG("==> stm_wbetl_validate(%p[%lu-%lu])\n", tx, (unsigned long)tx->start, (unsigned long)tx->end);

  /* Validate all read entries */
  r = tx->r_set.entries;
  for (i = tx->r_set.nb_entries; i > 0; i--, r++) {
    /* Read lock */
    l = ATOMIC_LOAD(r->lock);
    /* Unlocked and still the same version? */
    if (LOCK_GET_OWNED(l)) {
      /* Do we own the lock? */
      w_entry_t *w = (w_entry_t *)LOCK_GET_ADDR(l);
      /* Simply check if address falls inside our write set (avoids non-faulting load) */
      if (!(tx->w_set.entries <= w && w < tx->w_set.entries + tx->w_set.nb_entries))
      {
        return 0;
      }
      /* We own the lock: OK */
    } else {
      if (LOCK_GET_TIMESTAMP(l) != r->version) {
        /* Other version: cannot validate */
        return 0;
      }
      /* Same version: OK */
    }
  }
  return 1;
}

/*
 * Extend snapshot range.
 */
static INLINE int stm_wbetl_extend(stm_tx_t *tx)
{
  stm_word_t now;

  PRINT_DEBUG("==> stm_wbetl_extend(%p[%lu-%lu])\n", tx, (unsigned long)tx->start, (unsigned long)tx->end);
  
  /* Get current time */
  now = GET_CLOCK;
  /* No need to check clock overflow here. The clock can exceed up to MAX_THREADS and it will be reset when the quiescence is reached. */

  /* Try to validate read set */
  if (stm_wbetl_validate(tx)) {
    /* It works: we can extend until now */
    tx->end = now;
    return 1;
  }
  return 0;
}

static INLINE void stm_wbetl_rollback(stm_tx_t *tx)
{
  w_entry_t *w;
  int i;
  
  assert(IS_ACTIVE(tx->status));

  PRINT_DEBUG("==> stm_wbetl_rollback(%p[%lu-%lu])\n", tx, (unsigned long)tx->start, (unsigned long)tx->end);

  /* Drop locks */
  i = tx->w_set.nb_entries;
  if (i > 0) {
    w = tx->w_set.entries;
    for (; i > 0; i--, w++) {
      if (w->next == NULL) {
        /* Only drop lock for last covered address in write set */
        ATOMIC_STORE(w->lock, LOCK_SET_TIMESTAMP(w->version));
      }
    }
    /* Make sure that all lock releases become visible */
    ATOMIC_MB_WRITE;
  }
}

/*
 * Load a word-sized value (invisible read).
 */
static INLINE stm_word_t stm_wbetl_read_invisible(stm_tx_t *tx, volatile stm_word_t *addr, struct index_layer* layer)
{
	volatile stm_word_t *lock;
  	stm_word_t l, l2, value, version;
  	r_entry_t *r;
  	w_entry_t *w;

  	assert(IS_ACTIVE(tx->status));

	PRINT_DEBUG("==> stm_wbetl_read_invisible(t=%p[%lu-%lu],a=%p)\n", tx, (unsigned long)tx->start, (unsigned long)tx->end, addr);

  	/* Get reference to lock */
  	lock = GET_LOCK(addr);

  	/* Note: we could check for duplicate reads and get value from read set */

  	/* Read lock, value, lock */
  	l = ATOMIC_LOAD_ACQ(lock);

restart_no_load:
  	if (unlikely(LOCK_GET_WRITE(l))) {
    	/* case 1: address is locked */
		
    	/* Do we own the lock? */
    	w = (w_entry_t *)LOCK_GET_ADDR(l);
    	/* Simply check if address falls inside our write set (avoids non-faulting load) */
    	if (likely(tx->w_set.entries <= w && w < tx->w_set.entries + tx->w_set.nb_entries)) {
      		/* case I: Yes: did we previously write the same address? */
      		while (1) {
        		if (addr == w->addr) {
          			/* Yes: get value from write set (or from memory if mask was empty) */
          			value = (w->mask == 0 ? ATOMIC_LOAD(addr) : w->value);
          			break;
        		}
        		if (w->next == NULL) {
          			/* No: get value from memory */
                value = (stm_word_t)layer->lookup(layer->index_struct, (const void*)addr, KEY_LEN);
          			//value = ATOMIC_LOAD(addr);
          			break;
        		}
        		w = w->next;
      		}
      		/* No need to add to read set (will remain valid) */
      		return value;
    	}
    	/* case II: we do not own the lock, abort */
    	stm_rollback(tx, STM_ABORT_RW_CONFLICT);
    	return 0;
		
  	} else {
  		/* case 2: address is not locked */
		
		  /* load the value from memory */
    	//value = ATOMIC_LOAD_ACQ(addr);
      value = (stm_word_t)layer->lookup(layer->index_struct, (const void*)addr, KEY_LEN);
		  /* load the lock again */
    	l2 = ATOMIC_LOAD_ACQ(lock);
    	if (unlikely(l != l2)) {
			/* lock has been changed, restart */
      		l = l2;
      		goto restart_no_load;
    	}
	
    	/* Check timestamp */
    	version = LOCK_GET_TIMESTAMP(l);
    	/* Valid version? */
    	if (unlikely(version > tx->end)) {
      		/* case I: No: try to extend first (except for read-only transactions: no read set) */
      		if (tx->attr.read_only || !stm_wbetl_extend(tx)) {
        		/* Not much we can do: abort */
        		stm_rollback(tx, STM_ABORT_VAL_READ);
        		return 0;
      		}
      		/* Verify that version has not been overwritten (read value has not
       		* yet been added to read set and may have not been checked during
       		* extend) */
      		l2 = ATOMIC_LOAD_ACQ(lock);
      		if (l != l2) {
        		l = l2;
        		goto restart_no_load;
      		}
      		/* Worked: we now have a good version (version <= tx->end) */
    	}
  	}
	/* We have a good version: add to read set (update transactions) and return value */

  	if (!tx->attr.read_only) {
    	/* Add address and version to read set */
    	if (tx->r_set.nb_entries == tx->r_set.size)
      		stm_allocate_rs_entries(tx, 1);
	
    	r = &tx->r_set.entries[tx->r_set.nb_entries++];
    	r->version = version;
    	r->lock = lock;
  }

  return value;
}

static INLINE stm_word_t stm_wbetl_read(stm_tx_t *tx, volatile stm_word_t *addr, 
		struct index_struct* layer)
{
  return stm_wbetl_read_invisible(tx, addr, layer);
}

static INLINE w_entry_t * stm_wbetl_write(stm_tx_t *tx, volatile stm_word_t *addr, stm_word_t value, 
			stm_word_t mask)
{
  volatile stm_word_t *lock;
  stm_word_t l, version;
  w_entry_t *w;
  w_entry_t *prev = NULL;

  PRINT_DEBUG("==> stm_wbetl_write(t=%p[%lu-%lu],a=%p,d=%p-%lu,m=0x%lx)\n",
               tx, (unsigned long)tx->start, (unsigned long)tx->end, addr, (void *)value, (unsigned long)value, (unsigned long)mask);

  /* Get reference to lock */
  lock = GET_LOCK(addr);

  /* Try to acquire lock */
restart:
  l = ATOMIC_LOAD_ACQ(lock);
restart_no_load:
  if (unlikely(LOCK_GET_OWNED(l))) {
    /* case 1: address is locked */

    /* Do we own the lock? */
    w = (w_entry_t *)LOCK_GET_ADDR(l);
    /* Simply check if address falls inside our write set (avoids non-faulting load) */
    if (likely(tx->w_set.entries <= w && w < tx->w_set.entries + tx->w_set.nb_entries)) {
      /* case I: we own the lock */
      if (mask == 0) {
        /* No need to insert new entry or modify existing one */
        return w;
      }
      prev = w;
      /* Did we previously write the same address? */
      while (1) {
        if (addr == prev->addr) {
          /* case i: we wrote the address before, no need to add to write set */
          if (mask != ~(stm_word_t)0) {
            if (prev->mask == 0)
              prev->value = ATOMIC_LOAD(addr);
            value = (prev->value & ~mask) | (value & mask);
          }
          prev->value = value;
          prev->mask |= mask;
          return prev;
        }
        if (prev->next == NULL) {
          /* Remember last entry in linked list (for adding new entry) */
          break;
        }
        prev = prev->next;
      }
	  /* case ii: we did not write this address before */
      /* Get version from previous write set entry (all entries in linked list have same version) */
      version = prev->version;
      
      if (tx->w_set.nb_entries == tx->w_set.size) {
	  	/* tx is abort due to write set is full */
        stm_rollback(tx, STM_ABORT_EXTEND_WS);
	  }
	  /* add it to the writing set */
      w = &tx->w_set.entries[tx->w_set.nb_entries];

      goto do_write;
    }

    /* case II: we do not own the lock */
    stm_rollback(tx, STM_ABORT_WW_CONFLICT);
    return NULL;
  }
  
  /* case 2: address is not locked */
  /* Handle write after reads (before CAS) */
  version = LOCK_GET_TIMESTAMP(l);

acquire:
  if (unlikely(version > tx->end)) {
    /* We might have read an older version previously */
    if (unlikely(stm_has_read(tx, lock) != NULL)) {
      /* Read version must be older (otherwise, tx->end >= version) */
      /* Not much we can do: abort */
      stm_rollback(tx, STM_ABORT_VAL_WRITE);
      return NULL;
    }
  }
  /* Acquire lock (ETL) */
  if (unlikely(tx->w_set.nb_entries == tx->w_set.size))
  	/* tx is abort due to write set is full */
    stm_rollback(tx, STM_ABORT_EXTEND_WS);
  /* add it to the writing set */
  w = &tx->w_set.entries[tx->w_set.nb_entries];

  /* we use CAS try to acquire the lock */
  if (unlikely(ATOMIC_CAS_FULL(lock, l, LOCK_SET_ADDR_WRITE((stm_word_t)w)) == 0))
  	/* CAS fails */
    goto restart;

do_write:
  /* Add address to write set */
  w->addr = addr;
  w->mask = mask;
  w->lock = lock;
  if (unlikely(mask == 0)) {
    /* Do not write anything */
#ifndef NDEBUG
    w->value = 0;
#endif /* ! NDEBUG */
  } else {
    /* Remember new value */
    if (mask != ~(stm_word_t)0)
      value = (ATOMIC_LOAD(addr) & ~mask) | (value & mask);
    w->value = value;
  }
  w->version = version;
  w->next = NULL;
  if (prev != NULL) {
    /* Link new entry in list */
    prev->next = w;
  }
  tx->w_set.nb_entries++;
  tx->w_set.has_writes++;

  return w;
}

static INLINE int stm_wbetl_commit(stm_tx_t *tx)
{
  	w_entry_t *w;
  	stm_word_t t;
  	int i;

	PRINT_DEBUG("==> stm_wbetl_commit(%p[%lu-%lu])\n", tx, (unsigned long)tx->start, (unsigned long)tx->end);

  	/* Get commit timestamp (may exceed VERSION_MAX by up to MAX_THREADS) */
	t = FETCH_INC_CLOCK + 1;

  	/* Try to validate (only if a concurrent transaction has committed since tx->start) */
  	if (unlikely(tx->start != t - 1 && !stm_wbetl_validate(tx))) {
   		/* Cannot commit */
    	stm_rollback(tx, STM_ABORT_VALIDATE);
    	return 0;
  	}

#if 0
  	/* Install new versions, drop locks and set new timestamp */
  	w = tx->w_set.entries;
  	for (i = tx->w_set.nb_entries; i > 0; i--, w++) {
    	if (w->mask != 0) {
	  		/* store value to the address */
      		ATOMIC_STORE(w->addr, w->value);
    	}
    	/* Only drop lock for last covered address in write set */
    	if (w->next == NULL) {
			/* store new version */
        	ATOMIC_STORE_REL(w->lock, LOCK_SET_TIMESTAMP(t));
    	}
  	}
#endif

  persist_write_set(tx, t);

end:
  	return 1;
}

/*
 * Initialize the transaction descriptor before start or restart.
 */
static INLINE void int_stm_prepare(stm_tx_t *tx)
{
  /* Read/write set */
  /* has_writes / nb_acquired are the same field. */
  tx->w_set.has_writes = 0;
  /* tx->w_set.nb_acquired = 0; */
  tx->w_set.nb_entries = 0;
  tx->r_set.nb_entries = 0;

start:
  /* Start timestamp */
  tx->start = tx->end = GET_CLOCK; /* OPT: Could be delayed until first read/write */
  if (unlikely(tx->start >= VERSION_MAX)) {
    /* Block all transactions and reset clock */
    //stm_quiesce_barrier(tx, rollover_clock, NULL);
    goto start;
  }

  /* Set status */
  UPDATE_STATUS(tx->status, TX_ACTIVE);
}

/*
 * Store a word-sized value (return write set entry or NULL).
 */
static INLINE w_entry_t *stm_write(stm_tx_t *tx, volatile stm_word_t *addr, stm_word_t value, 
		stm_word_t mask)
{
  w_entry_t *w;
  assert(IS_ACTIVE(tx->status));

  w = stm_wbetl_write(tx, addr, value, mask);

  return w;
}

/*
 * Check if stripe has been read previously.
 */
static INLINE r_entry_t* stm_has_read(stm_tx_t *tx, volatile stm_word_t *lock)
{
  r_entry_t *r;
  int i;

  PRINT_DEBUG("==> stm_has_read(%p[%lu-%lu],%p)\n", tx, (unsigned long)tx->start, (unsigned long)tx->end, lock);

  /* Look for read */
  r = tx->r_set.entries;
  for (i = tx->r_set.nb_entries; i > 0; i--, r++) {
    if (r->lock == lock) {
      /* Return first match*/
      return r;
    }
  }
  return NULL;
}

/*
 * Check if address has been written previously.
 */
static INLINE w_entry_t* stm_has_written(stm_tx_t *tx, volatile stm_word_t *addr)
{
  w_entry_t *w;
  int i;

  PRINT_DEBUG("==> stm_has_written(%p[%lu-%lu],%p)\n", tx, (unsigned long)tx->start, (unsigned long)tx->end, addr);

  /* Look for write */
  w = tx->w_set.entries;
  for (i = tx->w_set.nb_entries; i > 0; i--, w++) {
    if (w->addr == addr) {
      return w;
    }
  }
  return NULL;
}

/*
 * (Re)allocate read set entries.
 */
static NOINLINE void stm_allocate_rs_entries(stm_tx_t *tx, int extend)
{

  PRINT_DEBUG("==> stm_allocate_rs_entries(%p[%lu-%lu],%d)\n", tx, (unsigned long)tx->start, (unsigned long)tx->end, extend);

  if (extend) {
    /* Extend read set */
    tx->r_set.size *= 2;
    tx->r_set.entries = (r_entry_t *)realloc(tx->r_set.entries, tx->r_set.size * sizeof(r_entry_t));
  } else {
    /* Allocate read set */
    tx->r_set.entries = (r_entry_t *)malloc_aligned(tx->r_set.size * sizeof(r_entry_t));
  }
}

/*
 * (Re)allocate write set entries.
 */
static NOINLINE void stm_allocate_ws_entries(stm_tx_t *tx, int extend)
{

  PRINT_DEBUG("==> stm_allocate_ws_entries(%p[%lu-%lu],%d)\n", tx, (unsigned long)tx->start, (unsigned long)tx->end, extend);

  if (extend) {
    /* Extend write set */
    /* Transaction must be inactive for WRITE_THROUGH or WRITE_BACK_ETL */
    tx->w_set.size *= 2;
    tx->w_set.entries = (w_entry_t *)realloc(tx->w_set.entries, tx->w_set.size * sizeof(w_entry_t));
  } else {
    /* Allocate write set */
    tx->w_set.entries = (w_entry_t *)malloc_aligned(tx->w_set.size * sizeof(w_entry_t));
  }
  /* Ensure that memory is aligned. */
  assert((((stm_word_t)tx->w_set.entries) & OWNED_MASK) == 0);
}

/*
 * Rollback transaction.
 */
static NOINLINE void stm_rollback(stm_tx_t *tx, unsigned int reason)
{
  assert(IS_ACTIVE(tx->status));

  PRINT_DEBUG("==> stm_rollback(%p[%lu-%lu])\n", tx, (unsigned long)tx->start, (unsigned long)tx->end);

  stm_wbetl_rollback(tx);

  /* Set status to ABORTED */
  SET_STATUS(tx->status, TX_ABORTED);

  /* Abort for extending the write set */
  if (unlikely(reason == STM_ABORT_EXTEND_WS)) {
    stm_allocate_ws_entries(tx, 1);
  }

  /* Reset nesting level */
  tx->nesting = 1;

  /* Don't prepare a new transaction if no retry. */
  if (tx->attr.no_retry || (reason & STM_ABORT_NO_RETRY) == STM_ABORT_NO_RETRY) {
    tx->nesting = 0;
    return;
  }

  /* Reset field to restart transaction */
  int_stm_prepare(tx);

  /* Jump back to transaction start */
  /* Note: ABI usually requires 0x09 (runInstrumented+restoreLiveVariable) */
  reason |= STM_PATH_INSTRUMENTED;
  LONGJMP(tx->env, reason);
}

/*
 * Called by the CURRENT thread to start a transaction.
 */
void bonsai_stm_start(stm_tx_t* stx, stm_tx_attr_t attr)
{
  	/* TODO Nested transaction attributes are not checked if they are coherent
   	 * with parent ones.  */

  	/* Increment nesting level */
  	if (stx->nesting++ > 0)
    	return;

  	/* Attributes */
	  stx->attr = attr;

  	/* Initialize transaction descriptor */
  	int_stm_prepare(stx);

  	SETJMP(stx->env);
}

/*
 * Called by the CURRENT thread to commit a transaction.
 */
int bonsai_stm_commit(stm_tx_t* stx)
{
  	/* Decrement nesting level */
  	if (unlikely(--stx->nesting > 0))
    	return 1;

  	assert(IS_ACTIVE(stx->status));

  	/* A read-only transaction can commit immediately */
  	if (unlikely(stx->w_set.nb_entries == 0))
    	goto end;

  	/* Update transaction */
  	stm_wbetl_commit(stx);

end:
  	/* Set status to COMMITTED */
  	SET_STATUS(stx->status, TX_COMMITTED);

  	return 1;
}

/*
 * Called by the CURRENT thread to abort a transaction.
 */
void bonsai_stm_abort(stm_tx_t* stx, int reason)
{
  stm_rollback(stx, reason | STM_ABORT_EXPLICIT);
}

/*
 * Called by the CURRENT thread to load a word-sized value.
 */
stm_word_t bonsai_stm_load(stm_tx_t* stx, volatile stm_word_t *addr, struct index_struct* layer)
{
  return stm_wbetl_read(stx, addr, layer);
}

/*
 * Called by the CURRENT thread to store a word-sized value.
 */
void bonsai_stm_store(stm_tx_t* stx, volatile stm_word_t *addr, stm_word_t value)
{
  stm_write(stx, addr, value, ~(stm_word_t)0);
}

/*
 * Called by the CURRENT thread to store part of a word-sized value.
 */
void stm_store2(stm_tx_t* stx, volatile stm_word_t *addr, stm_word_t value, stm_word_t mask)
{
  stm_write(stx, addr, value, mask);
}

/*
 * Called by the CURRENT thread to initialize thread-local STM data.
 */
stm_tx_t* stm_init_thread(void)
{
  /* Allocate descriptor */
  stm_tx_t* tx = (stm_tx_t *)malloc_aligned(sizeof(stm_tx_t));
  
  /* Set attribute */
  tx->attr = (stm_tx_attr_t)0;
  /* Set status (no need for CAS or atomic op) */
  tx->status = TX_IDLE;
  
  /* Read set */
  tx->r_set.nb_entries = 0;
  tx->r_set.size = RW_SET_SIZE;
  stm_allocate_rs_entries(tx, 0);
  
  /* Write set */
  tx->w_set.nb_entries = 0;
  tx->w_set.size = RW_SET_SIZE;
  /* has_writes / nb_acquired are the same field. */
  tx->w_set.has_writes = 0;
  /* tx->w_set.nb_acquired = 0; */

  stm_allocate_ws_entries(tx, 0);
  /* Nesting level */
  tx->nesting = 0;

  //stm_quiesce_enter_thread(tx);

  return tx;
}

/*
 * Called by the CURRENT thread to cleanup thread-local STM data.
 */
void stm_exit_thread(stm_tx_t* tx)
{
  /* Avoid finalizing again a thread */
  if (tx == NULL)
    return;

  //stm_quiesce_exit_thread(tx);

  free(tx->r_set.entries);
  free(tx->w_set.entries);
  free(tx);
}

void dtx_begin() {;}
void dtx_commit() {;};

/*
 * persist_write_set - use durable transaction to persist the execution result
 * of stm.
 */
void persist_write_set(stm_tx_t *stx, stm_word_t timestamp) {
	w_entry_t *w = stx->w_set.entries;
	int i;

	dtx_begin(); {
		for (i = stx->w_set.nb_entries; i > 0; i--, w++) {
    		/* Only drop lock for last covered address in write set */
    		if (w->next == NULL) {
				/* store new version */
        		//ATOMIC_STORE_REL(w->lock, LOCK_SET_TIMESTAMP(timestamp));
    		}
  		}
	} dtx_commit();
}