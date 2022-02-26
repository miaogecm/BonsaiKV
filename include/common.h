#ifndef COMMON_H
#define COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

// #define BONSAI_DEBUG
#define BONSAI_SUPPORT_UPDATE
//#define BONSAI_HASHSET_DEBUG

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <errno.h>

#ifndef LOCAL
typedef uint64_t 	pkey_t;
typedef uint64_t 	pval_t;

typedef uint8_t		        __le8;
typedef uint16_t	        __le16;
typedef uint32_t	        __le32;
typedef unsigned long long	__le64;
#else
#include <linux/types.h>

typedef uint64_t 	pkey_t;
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

#define LONG_KEY

#ifdef LONG_KEY
#define KEY_OFF_BITS    48
#define KEY_LEN_BITS    16
#endif

#ifndef LONG_KEY
typedef struct pentry {
    __le64 k;
    __le64 v;
} pentry_t;
#else
typedef struct pentry {
    __le64 k        :KEY_OFF_BITS;
    __le64 k_len    :KEY_LEN_BITS;
    __le64 v;
} pentry_t;

extern struct bonasi_info;
extern struct bonsai_info* bonsai;

static int key_cmp(pkey_t a, pkey_t b) {
#ifndef LONG_KEY
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
#else
    struct data_layer* layer = &(bonsai->d_layer);
    char* start = layer->key_start;
    char* desta = start + (a >> KEY_LEN_BITS) << KEY_LEN_BITS;
    char* destb = start + (b >> KEY_LEN_BITS) << KEY_LEN_BITS;
    char sa[a_len];
    char sb[b_len];
    memcpy(sa, desta, a_len);
    memcpy(sb, destb, b_len);
    return strcmp(sa, sb);
#endif
}

#endif
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

#if 0
#define bonsai_print(fmt, args ...)	 fprintf(stdout, fmt, ##args);
#else 
#define bonsai_print(fmt, args ...) do {} while(0);
#endif

#ifdef BONSAI_DEBUG
#define bonsai_debug(fmt, args ...)	 do {fprintf(stdout, fmt, ##args);} while (0);
#else
#define bonsai_debug(fmt, args ...) do {} while(0);
#endif

#ifdef __cplusplus
}
#endif

#endif
