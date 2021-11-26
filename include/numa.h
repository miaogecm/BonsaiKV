#ifndef NUMA_H
#define NUMA_H

enum {
    NUM_SOCKET = 1,
    NUM_PHYSICAL_CPU_PER_SOCKET = 8,
    SMT_LEVEL = 2,
    NUM_CPU = 8
};

const int OS_CPU_ID[NUM_SOCKET][NUM_PHYSICAL_CPU_PER_SOCKET][SMT_LEVEL] = {
    { /* socket id: 0 */
        { /* physical cpu id: 0 */
          0, 8,     },
        { /* physical cpu id: 1 */
          1, 9,     },
        { /* physical cpu id: 2 */
          2, 10,     },
        { /* physical cpu id: 3 */
          3, 11,     },
        { /* physical cpu id: 4 */
          4, 12,     },
        { /* physical cpu id: 5 */
          5, 13,     },
        { /* physical cpu id: 6 */
          6, 14,     },
        { /* physical cpu id: 7 */
          7, 15,     },
    },
};

const int CPU_NUMA_ID[NUM_CPU] = {0};
#endif
