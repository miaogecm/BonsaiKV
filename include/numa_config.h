#ifndef NUMA_CONFIG_H
#define NUMA_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

enum {
    NUM_SOCKET = 1,
    NUM_PHYSICAL_CPU_PER_SOCKET = 4,
    SMT_LEVEL = 2,
	NUM_CPU = 4,
};

static const int OS_CPU_ID[NUM_SOCKET][NUM_PHYSICAL_CPU_PER_SOCKET][SMT_LEVEL] = {
    { /* socket id: 0 */
        { /* physical cpu id: 0 */
          0, 8,     },
        { /* physical cpu id: 1 */
          1, 9,     },
        { /* physical cpu id: 2 */
          2, 10,     },
        { /* physical cpu id: 3 */
          3, 11,     },
    },
};

#ifdef __cplusplus
}
#endif

#endif
