#ifndef COMMON_H
#define COMMON_H

typedef uint64_t pkey_t;
typedef uint64_t pval_t;

typedef uint8_t		__le8
typedef uint16_t	__le16
typedef uint32_t	__le32
typedef uint64_t	__le64

#define cpu_to_le8(x)
#define cpu_to_le16(x)
#define cpu_to_le32(x)
#define cpu_to_le64(x)

#define __le8_to_cpu(x)
#define __le16_to_cpu(x)
#define __le32_to_cpu(x)
#define __le64_to_cpu(x)

typedef struct pentry {
    __le64 k;
    __le64 v;
} pentry_t;

#define GET_ENT(ptr) ((pentry_t*)ptr)
#define GET_KEY(ptr) (GET_ENT(ptr)->key)
#define GET_VALUE(ptr) (GET_ENT(ptr)->value)

#define ENOMEM		101 /* out-of memory */
#define ENOENT		102 /* no such entry */
#define EEXIST		103 /* key exist */
#define EOPEN		104 /* open file error */
#define EPMEMOBJ	105 /* create pmemobj error */
#define EMMAP		106 /* memory-map error */
#define ESIGNO		107 /* sigaction error */	

#endif
