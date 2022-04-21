#ifndef ATOMIC_H
#define ATOMIC_H

#ifdef __cplusplus
extern "C" {
#endif

typedef unsigned int  u32;
typedef unsigned long u64;

typedef struct {
    volatile u32 counter;
} atomic_t;

typedef struct {
	volatile u64 counter;
} atomic64_t;

#define LOCK_PREFIX "lock; "

#define ATOMIC_INIT(i)	{ (i) }

#define cmpxchg(addr,old,x)      	__sync_val_compare_and_swap(addr,old,x)
#define cmpxchg2(addr,old,x)		__sync_bool_compare_and_swap(addr,old,x)
#define xadd(addr,n)          		__sync_add_and_fetch(addr,n)
#define xadd2(addr,n)				__sync_fetch_and_add(addr, n)

#define __X86_CASE_B	1
#define __X86_CASE_W	2
#define __X86_CASE_L	4
#define __X86_CASE_Q	8

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

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

static inline void atomic_or(int i, atomic_t *v)
{
	asm volatile(LOCK_PREFIX "orl %1,%0"
			: "+m" (v->counter)
			: "ir" (i)
			: "memory");
}

static inline void atomic_and(int i, atomic_t *v)
{
	asm volatile(LOCK_PREFIX "andl %1,%0"
			: "+m" (v->counter)
			: "ir" (i)
			: "memory");
}

/* The 64-bit atomic type */

#define ATOMIC64_INIT(i)	{ (i) }

/**
 * atomic64_read - read atomic64 variable
 * @v: pointer of type atomic64_t
 *
 * Atomically reads the value of @v.
 * Doesn't imply a read memory barrier.
 */
static inline u64 atomic64_read(const atomic64_t *v)
{
	return (v)->counter;
}

/**
 * atomic64_set - set atomic64 variable
 * @v: pointer to type atomic64_t
 * @i: required value
 *
 * Atomically sets the value of @v to @i.
 */
static inline void atomic64_set(atomic64_t *v, u64 i)
{
    v->counter = i;
}

/**
 * atomic64_add - add integer to atomic64 variable
 * @i: integer value to add
 * @v: pointer to type atomic64_t
 *
 * Atomically adds @i to @v.
 */
static __always_inline void atomic64_add(u64 i, atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "addq %1,%0"
		     : "=m" (v->counter)
		     : "er" (i), "m" (v->counter));
}

/**
 * atomic64_sub - subtract the atomic64 variable
 * @i: integer value to subtract
 * @v: pointer to type atomic64_t
 *
 * Atomically subtracts @i from @v.
 */
static inline void atomic64_sub(u64 i, atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "subq %1,%0"
		     : "=m" (v->counter)
		     : "er" (i), "m" (v->counter));
}

/**
 * atomic64_inc - increment atomic64 variable
 * @v: pointer to type atomic64_t
 *
 * Atomically increments @v by 1.
 */
static __always_inline void atomic64_inc(atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "incq %0"
		     : "=m" (v->counter)
		     : "m" (v->counter));
}

/**
 * atomic64_dec - decrement atomic64 variable
 * @v: pointer to type atomic64_t
 *
 * Atomically decrements @v by 1.
 */
static __always_inline void atomic64_dec(atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "decq %0"
		     : "=m" (v->counter)
		     : "m" (v->counter));
}

/**
 * atomic64_add_return - add and return
 * @i: integer value to add
 * @v: pointer to type atomic64_t
 *
 * Atomically adds @i to @v and returns @i + @v
 */
static __always_inline u64 atomic64_add_return(u64 i, atomic64_t *v)
{
	return i + xadd(&v->counter, i);
}

static inline u64 atomic64_sub_return(u64 i, atomic64_t *v)
{
	return atomic64_add_return(-i, v);
}

static inline u64 atomic64_fetch_add(u64 i, atomic64_t *v)
{
	return xadd(&v->counter, i);
}

static inline u64 atomic64_fetch_sub(u64 i, atomic64_t *v)
{
	return xadd(&v->counter, -i);
}

#define atomic64_inc_return(v)  (atomic64_add_return(1, (v)))
#define atomic64_dec_return(v)  (atomic64_sub_return(1, (v)))

static inline u64 atomic64_cmpxchg(atomic64_t *v, u64 old, u64 new)
{
	return cmpxchg(&v->counter, old, new);
}

#define atomic64_try_cmpxchg atomic64_try_cmpxchg
static __always_inline int atomic64_try_cmpxchg(atomic64_t *v, const u64 *old, u64 new)
{
	return cmpxchg2(&v->counter, *old, new);
}

/**
 * atomic64_add_unless - add unless the number is a given value
 * @v: pointer of type atomic64_t
 * @a: the amount to add to v...
 * @u: ...unless v is equal to u.
 *
 * Atomically adds @a to @v, so u64 as it was not @u.
 * Returns the old value of @v.
 */
static inline int atomic64_add_unless(atomic64_t *v, u64 a, u64 u)
{
	u64 c = atomic64_read(v);
	do {
		if (unlikely(c == u))
			return 0;
	} while (!atomic64_try_cmpxchg(v, &c, c + a));
	return 1;
}

#define atomic64_inc_not_zero(v) atomic64_add_unless((v), 1, 0)

/*
 * atomic64_dec_if_positive - decrement by 1 if old value positive
 * @v: pointer of type atomic_t
 *
 * The function returns the old value of *v minus 1, even if
 * the atomic variable, v, was not decremented.
 */
static inline u64 atomic64_dec_if_positive(atomic64_t *v)
{
	u64 dec, c = atomic64_read(v);
	do {
		dec = c - 1;
		if (unlikely(dec < 0))
			break;
	} while (!atomic64_try_cmpxchg(v, &c, dec));
	return dec;
}

static inline void atomic64_and(u64 i, atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "andq %1,%0"
			: "+m" (v->counter)
			: "er" (i)
			: "memory");
}

static inline u64 atomic64_fetch_and(u64 i, atomic64_t *v)
{
	u64 val;

	do {
        val = atomic64_read(v);
	} while (!atomic64_try_cmpxchg(v, &val, val & i));
	return val;
}

static inline void atomic64_or(u64 i, atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "orq %1,%0"
			: "+m" (v->counter)
			: "er" (i)
			: "memory");
}

static inline u64 atomic64_fetch_or(u64 i, atomic64_t *v)
{
	u64 val;

	do {
        val = atomic64_read(v);
	} while (!atomic64_try_cmpxchg(v, &val, val | i));
	return val;
}

static inline void atomic64_xor(u64 i, atomic64_t *v)
{
	asm volatile(LOCK_PREFIX "xorq %1,%0"
			: "+m" (v->counter)
			: "er" (i)
			: "memory");
}

static inline u64 atomic64_fetch_xor(u64 i, atomic64_t *v)
{
	u64 val;

	do {
        val = atomic64_read(v);
	} while (!atomic64_try_cmpxchg(v, &val, val ^ i));
	return val;
}

#ifdef __cplusplus
}
#endif

#endif
