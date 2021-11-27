#ifndef PMEM_H
#define PMEM_H

#include <libpmemobj.h>

#define POBJ_POOL_SIZE		(10 * 1024ULL * 1024ULL * 1024ULL) // 10 GB
static char* dax_file[NUM_SOCKET] = {"/mnt/ext4/pool1",
                                        "/mnt/ext4/pool2"};
PMEMobjpool* pop[NUM_SOCKET];

POBJ_LAYOUT_BEGIN(BONSAI);
POBJ_LAYOUT_TOID(BONSAI, struct pnode_t);
POBJ_LAYOUT_END(BONSAI);

static inline int file_exists(const char *filename) {
    struct stat buffer;
    return stat(filename, &buffer);
}

static int init_pmdk() {
    int i;
    for (i = 0; i < NUM_SOCKET; i++) {
        int sds_write_value = 0;
        pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
        if (file_exists(dax_file[i]) != 0) {
            printf("create new pmdk pool%d.\n, i");
            if ((pop[i] = pmemobj_create(dax_file[i], POBJ_LAYOUT_NAME(BONSAI),
                                (uint64_t)POBJ_POOL_SIZE, 0666)) == NULL) {
                perror("fail to create pmdk pool%d\n", i);
                exit(0);
            }
        } else {
            printf("open existing pmdk pool%d.\n", i);
            if ((pop[i] = pmemobj_open(dax_file[i], POBJ_LAYOUT_NAME(BONSAI))) == NULL) {
                perror("failed to open pmdk pool%d.\n", i);
                exit(0);
            }
        }
    }

	return 0;
}

static int uninit_pmdk() {
	pmemobj_close(pop);
	return 0;
}
#define TOID_OFFSET(o) ((o).oid.off)

#endif
/*pmem.h*/