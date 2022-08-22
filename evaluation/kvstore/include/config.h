#ifndef KVSTORE_CONFIG_H
#define KVSTORE_CONFIG_H

#define NUM_CPU         48
#define NUM_THREADS     48
#define INTERLEAVED_CPU_NR

#define YCSB_KVLIB_PATH           "../../src/libbonsai.so"
//#define YCSB_KVLIB_PATH           "lib/listdb/cmake-build-release/liblistdb_dll.so"
//#define YCSB_KVLIB_PATH           "lib/pactree/cmake-build-release/src/libpactree.so"
//#define YCSB_KVLIB_PATH           "lib/fastfair/libfastfair.so"
//#define YCSB_KVLIB_PATH           "lib/viper/cmake-build-release/libviper_dll.so"
//#define YCSB_KVLIB_PATH           "lib/dptree/cmake-build-release/libdptree_dll.so"
//#define YCSB_KVLIB_PATH           "lib/masstree-kohler/cmake-build-release/libmasstree_dll.so"
//#define YCSB_KVLIB_PATH           "lib/masstree-rmind/cmake-build-release/libmasstree_dll.so"
#define YCSB_WORKLOAD_NAME        "a_str"
#define YCSB_IS_STRING_KEY        1
#define YCSB_VAL_LEN              8
//#define YCSB_VAL_LEN              16384

#ifdef INTERLEAVED_CPU_NR

#define SOCKET0_CPUSEQ      \
	0, 8, 16, 24, 32, 40,   \
    2, 10, 18, 26, 34, 42,  \
    4, 12, 20, 28, 36, 44,  \
    6, 14, 22, 30, 38, 46

#define SOCKETALL_CPUSEQ                            \
    0, 1, 8,  9,  16, 17, 24, 25, 32, 33, 40, 41,   \
    2, 3, 10, 11, 18, 19, 26, 27, 34, 35, 42, 43,   \
    4, 5, 12, 13, 20, 21, 28, 29, 36, 37, 44, 45,   \
	6, 7, 14, 15, 22, 23, 30, 31, 38, 39, 46, 47

#else

#define SOCKET0_CPUSEQ      \
    0, 4, 8, 12, 16, 20,    \
    1, 5, 9, 13, 17, 21,    \
    2, 6, 10, 14, 18, 22,   \
    3, 7, 11, 15, 19, 23

#define SOCKETALL_CPUSEQ                           \
    0, 24, 4, 28, 8, 32, 12, 36, 16, 40, 20, 44,   \
    1, 25, 5, 29, 9, 33, 13, 37, 17, 41, 21, 45,   \
    2, 26, 6, 30, 10, 34, 14, 38, 18, 42, 22, 46,  \
    3, 27, 7, 31, 11, 35, 15, 39, 19, 43, 23, 47

#endif

#if NUM_THREADS > NUM_CPU / 2
#define CPUSEQ SOCKETALL_CPUSEQ
#else
#define CPUSEQ SOCKET0_CPUSEQ
#endif

static int cpuseq[] = { CPUSEQ };


struct bonsai_config {
    int nr_user_cpus;
    int *user_cpus;
    int stm_support;
};

struct pacman_config {
    int num_workers;
};

struct lbtree_config {
    int nr_workers;
};

#endif //KVSTORE_CONFIG_H
