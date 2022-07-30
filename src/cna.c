// Copyright (c) 2021 Diogo Behrens, Antonio Paolillo
// SPDX-License-Identifier: MIT

/******************************************************************************
 * Compact NUMA-Aware (CNA) Lock by Dave Dice and Alex Kogan
 *   https://arxiv.org/abs/1810.05600
 *
 * This version of the code is implemented with C11 (stdatomic.h) and contains
 * a maximally relaxed combination of memory barriers for the IMM memory model.
 * The barriers were discovered with the VSync tool as described here:
 *   https://arxiv.org/abs/2111.15240
 *
 * With these barriers, CNA guarantees safety (mutual exclusion) and liveness
 * (await loop termination) on Armv8, RISC-V, Power and x86.
 *
 * To verify this code with GenMC 0.7 use the following command:
 *   genmc -mo -imm -check-liveness -disable-spin-assume cna-c11.c
 *
 * Note that the minimum number of threads is 4. With less threads than that,
 * some scenarios are not exercised.
 *****************************************************************************/
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <assert.h>
#include <stdint.h>

#include "cna.h"

#define cpu_relax() asm volatile("pause\n" : : : "memory")

static inline int current_numa_node() {
  unsigned long a, d, c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  return (c & 0xFFF000) >> 12;
}

static bool keep_lock_local() {
    return true;
}

static __thread cna_node_t cna_node;

static inline int get_chip() {
  unsigned long a, d, c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  return (c & 0xFFF000) >> 12;
}

void cna_lock_init(cna_lock_t *lock) {
    lock->tail = NULL;
}

void cna_lock(cna_lock_t *lock)
{
    cna_node_t *me = &cna_node;

    atomic_store_explicit(&me->next, 0, memory_order_relaxed);
    atomic_store_explicit(&me->socket, -1, memory_order_relaxed);
    atomic_store_explicit(&me->spin, 0, memory_order_relaxed);

    cna_node_t *tail = atomic_exchange_explicit(&lock->tail, me, memory_order_seq_cst);
    if (!tail) {
        atomic_store_explicit(&me->spin, 1, memory_order_relaxed);
        return;
    }

    atomic_store_explicit(&me->socket, current_numa_node(), memory_order_relaxed);
    atomic_store_explicit(&tail->next, me, memory_order_release);

    while (!atomic_load_explicit(&me->spin, memory_order_acquire))
        cpu_relax();
}

static cna_node_t* find_successor(cna_node_t *me)
{
    cna_node_t *next = atomic_load_explicit(&me->next, memory_order_relaxed);
    int mySocket = atomic_load_explicit(&me->socket, memory_order_relaxed);

    if (mySocket == -1)
        mySocket = current_numa_node();
    if (atomic_load_explicit(&next->socket, memory_order_relaxed) == mySocket)
       return next;

    cna_node_t *secHead = next;
    cna_node_t *secTail = next;
    cna_node_t *cur = atomic_load_explicit(&next->next, memory_order_acquire);

    while (cur) {
        if (atomic_load_explicit(&cur->socket, memory_order_relaxed) == mySocket) {
            if (atomic_load_explicit(&me->spin, memory_order_relaxed) > 1) {
                cna_node_t *_spin = (cna_node_t*) atomic_load_explicit(&me->spin, memory_order_relaxed);
                cna_node_t *_secTail = atomic_load_explicit(&_spin->secTail, memory_order_relaxed);
                atomic_store_explicit(&_secTail->next, secHead, memory_order_relaxed);
            } else {
                atomic_store_explicit(&me->spin, (uintptr_t) secHead, memory_order_relaxed);
            }
            atomic_store_explicit(&secTail->next, NULL, memory_order_relaxed);
            cna_node_t *_spin = (cna_node_t*) atomic_load_explicit(&me->spin, memory_order_relaxed);
            atomic_store_explicit(&_spin->secTail, secTail, memory_order_relaxed);
            return cur;
        }
        secTail = cur;
        cur = atomic_load_explicit(&cur->next, memory_order_acquire);
    }
    return NULL;
}

void cna_unlock(cna_lock_t *lock)
{
    cna_node_t *me = &cna_node;

    if (!atomic_load_explicit(&me->next, memory_order_acquire)) {
        if (atomic_load_explicit(&me->spin, memory_order_relaxed) == 1) {
            cna_node_t *local_me = me;
            if (atomic_compare_exchange_strong_explicit(&lock->tail, &local_me, NULL, memory_order_seq_cst, memory_order_seq_cst)) {
                return;
            }
        } else {
            cna_node_t *secHead = (cna_node_t *) atomic_load_explicit(&me->spin, memory_order_relaxed);
            cna_node_t *local_me = me;
            if (atomic_compare_exchange_strong_explicit(&lock->tail, &local_me,
                    atomic_load_explicit(&secHead->secTail, memory_order_relaxed),
                    memory_order_seq_cst, memory_order_seq_cst)) {
                atomic_store_explicit(&secHead->spin, 1, memory_order_release);
                return;
            }
        }
        while (atomic_load_explicit(&me->next, memory_order_relaxed) == NULL)
            cpu_relax();
    }
    cna_node_t *succ = NULL;
    if (keep_lock_local() && (succ = find_successor(me))) {
        atomic_store_explicit(&succ->spin,
            atomic_load_explicit(&me->spin, memory_order_relaxed),
            memory_order_release);
    } else if (atomic_load_explicit(&me->spin, memory_order_relaxed) > 1) {
        succ = (cna_node_t *) atomic_load_explicit(&me->spin, memory_order_relaxed);
        atomic_store_explicit(
            &atomic_load_explicit(&succ->secTail, memory_order_relaxed)->next,
            atomic_load_explicit(&me->next, memory_order_relaxed),
            memory_order_relaxed);
        atomic_store_explicit(&succ->spin, 1, memory_order_release);
    } else {
        succ = (cna_node_t*) atomic_load_explicit(&me->next, memory_order_relaxed);
        atomic_store_explicit(&succ->spin, 1, memory_order_release);
    }
}
