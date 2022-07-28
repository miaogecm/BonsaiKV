enum {
    NUM_SOCKET = 2,
    NUM_PHYSICAL_CPU_PER_SOCKET = 24,
    SMT_LEVEL = 1,
};

const int OS_CPU_ID[NUM_SOCKET][NUM_PHYSICAL_CPU_PER_SOCKET][SMT_LEVEL] = {
        { /* socket id: 0 */
                { /* physical cpu id: 0 */
                        0,     },
                { /* physical cpu id: 1 */
                        4,     },
                { /* physical cpu id: 2 */
                        8,     },
                { /* physical cpu id: 3 */
                        12,     },
                { /* physical cpu id: 4 */
                        10,     },
                { /* physical cpu id: 5 */
                        6,     },
                { /* physical cpu id: 6 */
                        2,     },
                { /* physical cpu id: 8 */
                        16,     },
                { /* physical cpu id: 9 */
                        20,     },
                { /* physical cpu id: 10 */
                        24,     },
                { /* physical cpu id: 11 */
                        22,     },
                { /* physical cpu id: 12 */
                        18,     },
                { /* physical cpu id: 13 */
                        14,     },
                { /* physical cpu id: 16 */
                        28,     },
                { /* physical cpu id: 17 */
                        32,     },
                { /* physical cpu id: 18 */
                        36,     },
                { /* physical cpu id: 19 */
                        34,     },
                { /* physical cpu id: 20 */
                        30,     },
                { /* physical cpu id: 21 */
                        26,     },
                { /* physical cpu id: 25 */
                        40,     },
                { /* physical cpu id: 26 */
                        44,     },
                { /* physical cpu id: 27 */
                        46,     },
                { /* physical cpu id: 28 */
                        42,     },
                { /* physical cpu id: 29 */
                        38,     },
        },
        { /* socket id: 1 */
                { /* physical cpu id: 0 */
                        1,     },
                { /* physical cpu id: 1 */
                        5,     },
                { /* physical cpu id: 2 */
                        9,     },
                { /* physical cpu id: 3 */
                        13,     },
                { /* physical cpu id: 4 */
                        11,     },
                { /* physical cpu id: 5 */
                        7,     },
                { /* physical cpu id: 6 */
                        3,     },
                { /* physical cpu id: 8 */
                        17,     },
                { /* physical cpu id: 9 */
                        21,     },
                { /* physical cpu id: 10 */
                        25,     },
                { /* physical cpu id: 11 */
                        23,     },
                { /* physical cpu id: 12 */
                        19,     },
                { /* physical cpu id: 13 */
                        15,     },
                { /* physical cpu id: 16 */
                        29,     },
                { /* physical cpu id: 17 */
                        33,     },
                { /* physical cpu id: 18 */
                        37,     },
                { /* physical cpu id: 19 */
                        35,     },
                { /* physical cpu id: 20 */
                        31,     },
                { /* physical cpu id: 21 */
                        27,     },
                { /* physical cpu id: 25 */
                        41,     },
                { /* physical cpu id: 26 */
                        45,     },
                { /* physical cpu id: 27 */
                        47,     },
                { /* physical cpu id: 28 */
                        43,     },
                { /* physical cpu id: 29 */
                        39,     },
        },
};
