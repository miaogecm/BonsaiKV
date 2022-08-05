/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include "cpu.h"

#include <stdio.h>
#include <fcntl.h>
#include <libpmem.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <numa.h>

#include "arch.h"
#include "bonsai.h"
#include "region.h"
#include "data_layer.h"
#include "config.h"
#include "valman.h"

static char *log_region_fpath[NUM_DIMM] = {
	"/mnt/ext4/dimm0/log",
	"/mnt/ext4/dimm1/log",
	"/mnt/ext4/dimm2/log",
	"/mnt/ext4/dimm3/log",
	"/mnt/ext4/dimm4/log",
	"/mnt/ext4/dimm5/log",
	"/mnt/ext4/dimm6/log",
	"/mnt/ext4/dimm7/log",
	"/mnt/ext4/dimm8/log",
	"/mnt/ext4/dimm9/log",
	"/mnt/ext4/dimm10/log",
	"/mnt/ext4/dimm11/log",
};

static char* pnode_region_fpath[NUM_DIMM] = {
	"/mnt/ext4/dimm0/pnopool",
	"/mnt/ext4/dimm1/pnopool",
	"/mnt/ext4/dimm2/pnopool",
	"/mnt/ext4/dimm3/pnopool",
	"/mnt/ext4/dimm4/pnopool",
	"/mnt/ext4/dimm5/pnopool",
	"/mnt/ext4/dimm6/pnopool",
	"/mnt/ext4/dimm7/pnopool",
	"/mnt/ext4/dimm8/pnopool",
	"/mnt/ext4/dimm9/pnopool",
	"/mnt/ext4/dimm10/pnopool",
	"/mnt/ext4/dimm11/pnopool",
};

static char* pval_region_fpath[NUM_DIMM] = {
	"/mnt/ext4/dimm0/pvalpool",
	"/mnt/ext4/dimm1/pvalpool",
	"/mnt/ext4/dimm2/pvalpool",
	"/mnt/ext4/dimm3/pvalpool",
	"/mnt/ext4/dimm4/pvalpool",
	"/mnt/ext4/dimm5/pvalpool",
	"/mnt/ext4/dimm6/pvalpool",
	"/mnt/ext4/dimm7/pvalpool",
	"/mnt/ext4/dimm8/pvalpool",
	"/mnt/ext4/dimm9/pvalpool",
	"/mnt/ext4/dimm10/pvalpool",
	"/mnt/ext4/dimm11/pvalpool",
};

int log_region_init(struct log_layer *layer) {
    size_t size_per_dimm = sizeof(struct dimm_log_region);
	int dimm, cpu, fd, ret = 0;
    void *vaddr;

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
		/* create a pmem file */
    	if ((fd = open(log_region_fpath[dimm], O_CREAT | O_RDWR, 0666)) < 0) {
            ret = errno;
        	perror("open");
        	goto out;
    	}

#ifndef USE_DEVDAX
    	/* allocate the pmem */
    	if (posix_fallocate(fd, 0, (off_t) size_per_dimm) != 0) {
            ret = errno;
        	perror("posix_fallocate");
        	goto out;
    	}
#endif

    	/* memory map it, a page for metadata */
    	if ((vaddr = mmap(NULL, ALIGN(size_per_dimm, PMM_PAGE_SIZE),
                          PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == MAP_FAILED) {
            ret = errno;
       		perror("mmap");
        	goto out;
    	}

		layer->dimm_regions[dimm] = vaddr;
		layer->dimm_region_fd[dimm] = fd;

		for (cpu = 0; cpu < NUM_CPU_PER_DIMM; cpu ++) {
			layer->dimm_regions[dimm]->regions[cpu].meta.start = 
				layer->dimm_regions[dimm]->regions[cpu].meta.end = 0;
		}

		bonsai_print("log_region_init dimm[%d] region: [%016lx, %016lx]\n",
                     dimm, (unsigned long) vaddr, (unsigned long) vaddr + size_per_dimm);
	}

out:
	return ret;
}

void log_region_deinit(struct log_layer* layer) {
    size_t size_per_dimm = sizeof(struct dimm_log_region);
	int dimm;

	free(layer->desc);

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
		munmap(layer->dimm_regions[dimm], ALIGN(size_per_dimm, PMM_PAGE_SIZE));
		close(layer->dimm_region_fd[dimm]);
	}
}

int pnode_region_init(struct data_layer *layer) {
  	size_t size_per_dimm = DATA_REGION_SIZE / NUM_DIMM_PER_SOCKET;
	struct data_region *region;
	int dimm, fd, ret = 0;
    void *vaddr;

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
			region = &layer->pno_region[dimm];

			/* create a pmem file */
    	if ((fd = open(pnode_region_fpath[dimm], O_CREAT | O_RDWR, 0666)) < 0) {
            ret = errno;
        	perror("open");
        	goto out;
    	}

#ifndef USE_DEVDAX
    	/* allocate the pmem */
    	if (posix_fallocate(fd, 0, (off_t) size_per_dimm) != 0) {
            ret = errno;
        	perror("posix_fallocate");
        	goto out;
    	}
#endif

    	/* memory map it */
    	if ((vaddr = mmap(NULL, ALIGN(size_per_dimm, PMM_PAGE_SIZE),
                          PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == MAP_FAILED) {
            ret = errno;
       		perror("mmap");
        	goto out;
    	}

		region->d_fd = fd;
		region->d_start = vaddr;

		bonsai_print("pnode_region_init dimm[%d] region: [%016lx, %016lx]\n",
                     dimm, (unsigned long) region->d_start,
                     (unsigned long) region->d_start + size_per_dimm);
	}

out:
	return ret;
}

void pnode_region_deinit(struct data_layer *layer) {
    size_t size_per_dimm = DATA_REGION_SIZE / NUM_DIMM_PER_SOCKET;
	struct data_region *region;
	int dimm;

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
			region = &layer->pno_region[dimm];
      munmap(region->d_start, ALIGN(size_per_dimm, PMM_PAGE_SIZE));
			close(region->d_fd);
	}
}

#ifdef STR_VAL

int pval_region_init(struct data_layer *layer) {
  	size_t size_per_dimm = valman_vpool_dimm_size();
	struct data_region *region;
	int dimm, fd, ret = 0;
    void *vaddr;

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
			region = &layer->val_region[dimm];

			/* create a pmem file */
    	if ((fd = open(pval_region_fpath[dimm], O_CREAT | O_RDWR, 0666)) < 0) {
            ret = errno;
        	perror("open");
        	goto out;
    	}

#ifndef USE_DEVDAX
    	/* allocate the pmem */
    	if (posix_fallocate(fd, 0, (off_t) size_per_dimm) != 0) {
            ret = errno;
        	perror("posix_fallocate");
        	goto out;
    	}
#endif

    	/* memory map it */
    	if ((vaddr = mmap(NULL, ALIGN(size_per_dimm, PMM_PAGE_SIZE),
                          PROT_READ | PROT_WRITE, MAP_FILE | MAP_SHARED, fd, 0)) == MAP_FAILED) {
            ret = errno;
       		perror("mmap");
        	goto out;
    	}

		region->d_fd = fd;
		region->d_start = vaddr;

		bonsai_print("pval_region_init dimm[%d] region: [%016lx, %016lx]\n",
                     dimm, (unsigned long) region->d_start,
                            (unsigned long) region->d_start + size_per_dimm);
	}

out:
	return ret;
}

void pval_region_deinit(struct data_layer *layer) {
  	size_t size_per_dimm = valman_vpool_dimm_size();
	struct data_region *region;
	int dimm;

	for (dimm = 0; dimm < NUM_DIMM; dimm++) {
        region = &layer->val_region[dimm];
        munmap(region->d_start, ALIGN(size_per_dimm, PMM_PAGE_SIZE));
        close(region->d_fd);
	}
}

#endif

int data_region_init(struct data_layer *layer) {
    int err;

    if (unlikely(err = pnode_region_init(layer))) {
        goto out;
    }

#ifdef STR_VAL
    if (unlikely(err = pval_region_init(layer))) {
        goto out;
    }
#endif

out:
    return err;
}

void data_region_deinit(struct data_layer *layer) {
#ifdef STR_VAL
    pval_region_deinit(layer);
#endif
    pnode_region_deinit(layer);
}
