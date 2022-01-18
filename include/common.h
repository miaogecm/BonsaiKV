#ifndef COMMON_H
#define COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

#define BONSAI_SUPPORT_UPDATE
//#define BONSAI_HASHSET_DEBUG

#include <stdint.h>
#include <stddef.h>

#ifndef LOCAL
typedef uint64_t 	pkey_t;
typedef uint64_t 	pval_t;

typedef uint8_t		__le8;
typedef uint16_t	__le16;
typedef uint32_t	__le32;
typedef uint64_t	__le64;
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

typedef struct pentry {
    __le64 k;
    __le64 v;
} pentry_t;

#ifndef ENOMEM
#define ENOMEM		101 /* out-of memory */
#endif
#ifndef ENOENT
#define ENOENT		102 /* no such entry */
#endif
#ifndef EEXIST
#define EEXIST		103 /* key exist */
#endif
#define EOPEN		104 /* open file error */
#define EPMEMOBJ	105 /* create pmemobj error */
#define EMMAP		106 /* memory-map error */
#define ESIGNO		107 /* sigaction error */
#define ETHREAD		108 /* thread create error */

#ifndef likely
#define likely(x) __builtin_expect((unsigned long)(x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect((unsigned long)(x), 0)
#endif

#ifdef BONSAI_DEBUG
#define kv_debug(fmt, args ...)	 do {fprintf(stderr, fmt, ##args);} while (0)
#else
#define kv_debug(fmt, args ...) do{}while(0)
#endif

#ifdef __cplusplus
}
#endif

#endif
