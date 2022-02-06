#ifndef UTIL_H
#define UTIL_H

#define SYNC_SWAP(addr,x)         __sync_lock_test_and_set(addr,x)
#define SYNC_CAS(addr,old,x)      __sync_val_compare_and_swap(addr,old,x)
#define SYNC_ADD(addr,n)          __sync_add_and_fetch(addr,n)
#define SYNC_SUB(addr,n)          __sync_sub_and_fetch(addr,n)
#define ACCESS_ONCE(x) (*(volatile typeof(x) *)&(x))

#endif