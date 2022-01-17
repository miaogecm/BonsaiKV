#ifndef ATOMIC_H
#define ATOMIC_H

#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned int u32;

typedef struct {
    volatile u32 counter;
} atomic_t;

#define LOCK_PREFIX "lock; "

#define ATOMIC_INIT(i)	{ (i) }

#define cmpxchg(addr,old,x)      	__sync_val_compare_and_swap(addr,old,x)
#define cmpxchg2(addr,old,x)		__sync_bool_compare_and_swap(addr,old,x)
#define xadd(addr,n)          		__sync_add_and_fetch(addr,n)
#define xadd2(addr,n)				__sync_fetch_and_add(addr, n)

#define barrier()	 				__asm__ __volatile__("": : :"memory")

#define __X86_CASE_B	1
#define __X86_CASE_W	2
#define __X86_CASE_L	4
#define __X86_CASE_Q	8

static inline void memory_mfence() {
    __asm__ __volatile__("mfence" : : : "memory");
}

static inline void memory_lfence() {
    __asm__ __volatile__("lfence" : : : "memory");
}

static inline void memory_sfence() {
    __asm__ __volatile__("sfence" : : : "memory");
}

#define ATOMIC_CAS_FULL(a, e, v)      	(__sync_bool_compare_and_swap(a, e, v))
#define ATOMIC_FETCH_INC_FULL(a)      	(__sync_fetch_and_add(a, 1))
#define ATOMIC_FETCH_DEC_FULL(a)      	(__sync_fetch_and_add(a, -1))
#define ATOMIC_FETCH_ADD_FULL(a, v)   	(__sync_fetch_and_add(a, v))
#define ATOMIC_LOAD_ACQ(a)            	(*(a))
#define ATOMIC_LOAD(a)                	(*(a))
#define ATOMIC_STORE_REL(a, v)        	(*(a) = (v))
#define ATOMIC_STORE(a, v)            	(*(a) = (v))
#define ATOMIC_MB_READ                	memory_lfence()
#define ATOMIC_MB_WRITE               	memory_sfence()
#define ATOMIC_MB_FULL					memory_mfence()
#define ATOMIC_CB						__asm__ __volatile__("": : :"memory")

#define ACCESS_ONCE(x) (* (volatile typeof(x) *) &(x))

#define __xchg_op(ptr, arg, op, lock)					\
	({								\
	        __typeof__ (*(ptr)) __ret = (arg);			\
		switch (sizeof(*(ptr))) {				\
		case __X86_CASE_B:					\
			asm volatile (lock #op "b %b0, %1\n"		\
				      : "+q" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
			break;						\
		case __X86_CASE_W:					\
			asm volatile (lock #op "w %w0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
			break;						\
		case __X86_CASE_L:					\
			asm volatile (lock #op "l %0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
			break;						\
		case __X86_CASE_Q:					\
			asm volatile (lock #op "q %q0, %1\n"		\
				      : "+r" (__ret), "+m" (*(ptr))	\
				      : : "memory", "cc");		\
			break;						\
		}							\
		__ret;							\
	})

//#define __xadd(ptr, inc, lock)	__xchg_op((ptr), (inc), xadd, lock)
//#define xadd(ptr, inc)		__xadd((ptr), (inc), LOCK_PREFIX)

/**
 * atomic_read - read atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically reads the value of @v.
 */
static inline u32 atomic_read(const atomic_t *v)
{
    return v->counter;
}

/**
 * atomic_set - set atomic variable
 * @v: pointer of type atomic_t
 * @i: required value
 *
 * Atomically sets the value of @v to @i.
 */
static inline void atomic_set(atomic_t *v, u32 i)
{
    v->counter = i;
}

/**
 * atomic_inc - increment atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically increments @v by 1.
 */
static inline void atomic_inc(atomic_t *v)
{
    asm volatile(LOCK_PREFIX "incl %0"
    : "+m" (v->counter));
}

/**
 * atomic_dec - decrement atomic variable
 * @v: pointer of type atomic_t
 *
 * Atomically decrements @v by 1.
 */
static inline void atomic_dec(atomic_t *v)
{
    asm volatile(LOCK_PREFIX "decl %0"
    : "+m" (v->counter));
}

/**
 * atomic_dec_and_test - decrement and test
 * @v: pointer of type atomic_t
 *
 * Atomically decrements @v by 1 and
 * returns true if the result is 0, or false for all other
 * cases.
 */
static inline int atomic_dec_and_sign(atomic_t *v)
{
    unsigned char c;

    asm volatile(LOCK_PREFIX "decl %0; sets %1"
    : "+m" (v->counter), "=qm" (c)
    : : "memory");
    return c != 0;
}

/**
 * atomic_inc_and_test - increment and test
 * @v: pointer of type atomic_t
 *
 * Atomically increments @v by 1
 * and returns true if the result is zero, or false for all
 * other cases.
 */
static inline int atomic_inc_and_zero(atomic_t *v)
{
    unsigned char c;

    asm volatile(LOCK_PREFIX "incl %0; sete %1"
    : "+m" (v->counter), "=qm" (c)
    : : "memory");
    return c != 0;
}

/**
 * atomic_cmpxchg - atomic compare and swap
 * @v: pointer of type atomic_t
 * @old: old value
 * @new: new value
 *
 * Atomically compare @v with @old,
 * and returns *v.
 */
static inline int atomic_cmpxchg(atomic_t *v, int old, int new)
{
    return cmpxchg(&v->counter, old, new);
}

/**
 * atomic_add - add integer to atomic variable
 * @i: integer value to add
 * @v: pointer of type atomic_t
 *
 * Atomically adds @i to @v.
 */
static inline void atomic_add(int i, atomic_t *v)
{
    asm volatile(LOCK_PREFIX "addl %1,%0"
    : "+m" (v->counter)
    : "ir" (i));
}

/**
 * atomic_sub - subtract integer from atomic variable
 * @i: integer value to subtract
 * @v: pointer of type atomic_t
 *
 * Atomically subtracts @i from @v.
 */
static inline void atomic_sub(int i, atomic_t *v)
{
    asm volatile(LOCK_PREFIX "subl %1,%0"
    : "+m" (v->counter)
    : "ir" (i));
}

/**
 * atomic_add_return - add integer and return
 * @i: integer value to add
 * @v: pointer of type atomic_t
 *
 * Atomically adds @i to @v and returns @i + @v
 */
static inline int atomic_add_return(int i, atomic_t *v)
{
    int __i;
    /* Modern 486+ processor */
    __i = i;
    asm volatile(LOCK_PREFIX "xaddl %0, %1"
    : "+r" (i), "+m" (v->counter)
    : : "memory");
    return i + __i;
}

/**
 * atomic_sub_return - subtract integer and return
 * @v: pointer of type atomic_t
 * @i: integer value to subtract
 *
 * Atomically subtracts @i from @v and returns @v - @i
 */
static inline int atomic_sub_return(int i, atomic_t *v)
{
    return atomic_add_return(-i, v);
}

#ifdef __cplusplus
}
#endif

#endif
