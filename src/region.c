/*
 * Bonsai: Transparent and Efficient DRAM Index Structure Transplant for NVM
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 *	   	   Kangyue Gao: xxxx@gmail.com
 */
#include <stdio.h>
#include <fcntl.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>    

#include "bonsai.h"
#include "region.h"
#include "numa_config.h"

static char *log_region_fpath[NUM_SOCKET] = {
	"/mnt/node0/region0",
	"/mnt/node1/region1"
}

static char* data_region_fpath[NUM_SOCKET] = {
	"/mnt/node0/objpool0",
	"/mnt/node1/objpool1"
}

static void free_log_page(struct log_region *region, struct log_page_desc* page) {
	page = inuse;
	region->inuse = LOG_REGION_OFF_TO_ADDR(region, page->p_next);

	region->free->p_next = LOG_REGION_ADDR_TO_OFF(region, page);
	region->free = page;

	page->p_num_log = 0;
}

struct log_page_desc* alloc_log_page(struct log_region *region) {
	struct log_page_desc* free = region->free;
	struct log_page_desc* inuse = region->inuse;
	struct log_page_desc* page;
	
	page = free;
	free = LOG_REGION_OFF_TO_ADDR(region, page->p_next);

	page->p_next = inuse->p_next;
	inuse->p_next = LOG_REGION_ADDR_TO_OFF(region, page);

	return page;
}

static inline void init_log_page(struct log_page_desc* page, off_t page_off, int last) {
	page->p_off = page_off;
	page->p_num_log = 0;
	page->p_next = last ? (page_off + PAGE_SIZE) : 0;
}

static void init_per_cpu_log_region(struct log_region* region, struct log_region_desc *desc, 
			char *paddr, off_t offset, size_t size) {
	int i, num_page = LOG_REGION_SIZE / NUM_PHYSICAL_CPU_PER_SOCKET / PAGE_SIZE;

	desc->r_off = offset;
	desc->r_size = size;
	desc->r_oplog_top = 0;

	memset(paddr, 0, size);

	region->free = (struct log_page_desc*)paddr;
	region->inuse = NULL;

	for (i = 0; i < num_page; i++) {
		init_log_page((struct log_page_desc*)(paddr + PAGE_SIZE), 
						offset + PAGE_SIZE, 
						(i == num_page - 1) ? 1 : 0);
	}
}

int log_region_deinit(struct log_layer* layer) {
	struct log_region *region;
	int node, cpu, fd, error = 0;

	for (node = 0; node < NUM_SOCKET; node ++) {
		pmem_unmap(layer->pmem_addr[node], PMEM_SIZE);
		close(layer->pmem_fd[node]);
	}

	return error;
}

int log_region_init(struct log_layer* layer, struct bonsai_desc* bonsai) {
	struct log_region_desc *desc;
	struct log_region *region;
	int node, cpu, smt, fd, error = 0;
	size_t size_per_cpu = LOG_REGION_SIZE / NUM_PHYSICAL_CPU_PER_SOCKET;
	char* pmemaddr;

	for (node = 0; node < NUM_SOCKET; node ++) {
		/* create a pmem file */
		if ((fd = open(log_region_fpath[node], O_CREAT|O_RDWR, 0666)) < 0) {
			perror("open %s error\n", log_region_fpath[node]);
			error = -EOPEN;
			goto out;
		}
		memcpy(&bonsai->log_region_fpath[node], pmem_fpath, REGION_FPATH_LEN);

		if ((errno = posix_fallocate(fd, 0, PMEM_LEN)) != 0) {
			perror("posix_fallocate");
			goto out;
		}

		if ((pmemaddr = pmem_map(fd)) == NULL) {
			perror("pmem_map");
			goto out;
		}
		layer->pmem_addr[node] = pmemaddr;

		for (cpu = 0; cpu < NUM_PHYSICAL_CPU_PER_SOCKET; cpu ++) {
			region = layer->region[cpu];
			desc = &bonsai->log_region[OS_CPU_ID[node][cpu][0]];
			region->desc = desc;

			layer->region_start[cpu] = pmemaddr;

			init_per_cpu_log_region(region, desc, pmemaddr, 
				pmemaddr - layer->pmem_addr[node], size_per_cpu);
			pmemaddr += size_per_cpu;
		}

		layer->pmem_fd[node] = fd;
	}

out:
	return error;
}

int data_region_init(struct data_layer *layer) {
	struct data_region *region;
	int node, sds_write_value = 0;
	PMEMobjpool pop;

	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

	for (node = 0; node < NUM_SOCKET; node ++) {
		region = layer->region[node];
		if (file_exists(data_region_fpath[node]) != 0) {
			printf("create a new pmdk pool\n");
		
		if ((pop = pmemobj_create(dax_file, POBJ_LAYOUT_NAME(bonsai##node),
                              (uint64_t)DATA_REGION_SIZE, 0666)) == NULL) {
			perror("fail to create object pool\n");
			return -EPMEMOBJ;
		}

		region->pop = pop;
	}

	return 0;
}
