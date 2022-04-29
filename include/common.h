#ifndef COMMON_H
#define COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#define BONSAI_SUPPORT_UPDATE
//#define BONSAI_DEBUG

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

#ifndef LOCAL
typedef uint64_t 	pval_t;

typedef uint8_t		        __le8;
typedef uint16_t	        __le16;
typedef uint32_t	        __le32;
typedef unsigned long long	__le64;
#else
#include <linux/types.h>

typedef uint64_t 	pval_t;


typedef uint8_t		__le8;
// typedef uint16_t	__le16;
// typedef uint32_t	__le32;
// typedef uint64_t	__le64;
#endif

#define cpu_to_le8(x)		(x)
#define cpu_to_le16(x)		(x)
#define cpu_to_le32(x)		(x)
#define cpu_to_le64(x)		(x)

#define __le8_to_cpu(x)		(x)
#define __le16_to_cpu(x)	(x)
#define __le32_to_cpu(x)	(x)
#define __le64_to_cpu(x)	(x)

//#define STR_KEY

#ifdef STR_KEY

#define KEY_LEN             24

#define MAX_KEY             ((pkey_t) { "\x7f" })
#define MIN_KEY             ((pkey_t) { "" })

#define print_key(_key) 	printf("%s\n", _key.key);

#else

#define KEY_LEN             8

#define INT2KEY(val)        (* (pkey_t *) (unsigned long []) { (val) })

#define MAX_KEY             INT2KEY(-1UL)
#define MIN_KEY             INT2KEY(0UL)

#define print_key(_key) 	printf("%lu\n", *(unsigned long *) _key.key);

#endif

typedef struct {
    char key[KEY_LEN];
} pkey_t;

typedef struct pentry {
    pkey_t k;
	__le64 v;
} __attribute__((packed)) pentry_t;

#define EOPEN		104 /* open file error */
#define EPMEMOBJ	105 /* create pmemobj error */
#define EMMAP		106 /* memory-map error */
#define ESIGNO		107 /* sigaction error */
#define ETHREAD		108 /* thread create error */
#define EEXIT		109 /* exit */

#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif

#if 1
#define bonsai_print(fmt, args ...)	 fprintf(stdout, fmt, ##args)
#else 
#define bonsai_print(fmt, args ...) do {} while(0)
#endif

#ifdef BONSAI_DEBUG
#define bonsai_debug(fmt, args ...)	 do {fprintf(stdout, fmt, ##args);} while (0)
#else
#define bonsai_debug(fmt, args ...) do {} while(0)
#endif

/* Indirect macros required for expanded argument pasting, eg. __LINE__. */
#define ___PASTE(a,b) a##b
#define __PASTE(a,b) ___PASTE(a,b)

#define __UNIQUE_ID(prefix) __PASTE(__PASTE(__UNIQUE_ID_, prefix), __COUNTER__)

/*
 * min()/max()/clamp() macros that also do
 * strict type-checking.. See the
 * "unnecessary" pointer comparison.
 */
#define __min(t1, t2, min1, min2, x, y) ({		\
	t1 min1 = (x);					\
	t2 min2 = (y);					\
	(void) (&min1 == &min2);			\
	min1 < min2 ? min1 : min2; })

/**
 * min - return minimum of two values of the same or compatible types
 * @x: first value
 * @y: second value
 */
#define min(x, y)					\
	__min(typeof(x), typeof(y),			\
	      __UNIQUE_ID(min1_), __UNIQUE_ID(min2_),	\
	      x, y)

#define __max(t1, t2, max1, max2, x, y) ({		\
	t1 max1 = (x);					\
	t2 max2 = (y);					\
	(void) (&max1 == &max2);			\
	max1 > max2 ? max1 : max2; })

/**
 * max - return maximum of two values of the same or compatible types
 * @x: first value
 * @y: second value
 */
#define max(x, y)					\
	__max(typeof(x), typeof(y),			\
	      __UNIQUE_ID(max1_), __UNIQUE_ID(max2_),	\
	      x, y)

/**
 * min3 - return minimum of three values
 * @x: first value
 * @y: second value
 * @z: third value
 */
#define min3(x, y, z) min((typeof(x))min(x, y), z)

/**
 * max3 - return maximum of three values
 * @x: first value
 * @y: second value
 * @z: third value
 */
#define max3(x, y, z) max((typeof(x))max(x, y), z)

/**
 * min_not_zero - return the minimum that is _not_ zero, unless both are zero
 * @x: value1
 * @y: value2
 */
#define min_not_zero(x, y) ({			\
	typeof(x) __x = (x);			\
	typeof(y) __y = (y);			\
	__x == 0 ? __y : ((__y == 0) ? __x : min(__x, __y)); })

/**
 * clamp - return a value clamped to a given range with strict typechecking
 * @val: current value
 * @lo: lowest allowable value
 * @hi: highest allowable value
 *
 * This macro does strict typechecking of @lo/@hi to make sure they are of the
 * same type as @val.  See the unnecessary pointer comparisons.
 */
#define clamp(val, lo, hi) min((typeof(val))max(val, lo), hi)

/*
 * ..and if you can't take the strict
 * types, you can specify one yourself.
 *
 * Or not use min/max/clamp at all, of course.
 */

/**
 * min_t - return minimum of two values, using the specified type
 * @type: data type to use
 * @x: first value
 * @y: second value
 */
#define min_t(type, x, y)				\
	__min(type, type,				\
	      __UNIQUE_ID(min1_), __UNIQUE_ID(min2_),	\
	      x, y)

/**
 * max_t - return maximum of two values, using the specified type
 * @type: data type to use
 * @x: first value
 * @y: second value
 */
#define max_t(type, x, y)				\
	__max(type, type,				\
	      __UNIQUE_ID(min1_), __UNIQUE_ID(min2_),	\
	      x, y)

/**
 * clamp_t - return a value clamped to a given range using a given type
 * @type: the type of variable to use
 * @val: current value
 * @lo: minimum allowable value
 * @hi: maximum allowable value
 *
 * This macro does no typechecking and uses temporary variables of type
 * @type to make all the comparisons.
 */
#define clamp_t(type, val, lo, hi) min_t(type, max_t(type, val, lo), hi)

/**
 * clamp_val - return a value clamped to a given range using val's type
 * @val: current value
 * @lo: minimum allowable value
 * @hi: maximum allowable value
 *
 * This macro does no typechecking and uses temporary variables of whatever
 * type the input argument @val is.  This is useful when @val is an unsigned
 * type and @lo and @hi are literals that will otherwise be assigned a signed
 * integer type.
 */
#define clamp_val(val, lo, hi) clamp_t(typeof(val), val, lo, hi)


/**
 * swap - swap values of @a and @b
 * @a: first value
 * @b: second value
 */
#define swap(a, b) \
	do { typeof(a) __tmp = (a); (a) = (b); (b) = __tmp; } while (0)

#define __ALIGN_KERNEL(x, a)            __ALIGN_KERNEL_MASK(x, (typeof(x))(a) - 1)
#define __ALIGN_KERNEL_MASK(x, mask)    (((x) + (mask)) & ~(mask))

#define ALIGN(x, a)             __ALIGN_KERNEL((x), (a))
#define ALIGN_DOWN(x, a)        __ALIGN_KERNEL((x) - ((a) - 1), (a))
#define __ALIGN_MASK(x, mask)   __ALIGN_KERNEL_MASK((x), (mask))
#define PTR_ALIGN(p, a)         ((typeof(p))ALIGN((unsigned long)(p), (a)))
#define PTR_ALIGN_DOWN(p, a)    ((typeof(p))ALIGN_DOWN((unsigned long)(p), (a)))
#define IS_ALIGNED(x, a)        (((x) & ((typeof(x))(a) - 1)) == 0)

#ifdef __cplusplus
}
#endif

#endif
