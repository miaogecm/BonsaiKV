/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai: mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <libpmem.h>
#include <unistd.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>    

#include "arch.h"
#include "bonsai.h"
#include "region.h"
#include "cpu.h"
#include "numa_config.h"

#define TOID_OFFSET(o) ((o).oid.off)
/*
static char *log_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/region0",
	"/mnt/ext4/node1/region1"
};

static char* data_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/objpool0",
	"/mnt/ext4/node1/objpool1"
};
*/
static char *log_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/region0"
};

static char* data_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/objpool0"
};

void free_log_page(struct log_region *region, struct log_page_desc* page) {
	
	page = region->inuse;
	region->inuse = (struct log_page_desc*)LOG_REGION_OFF_TO_ADDR(region, page->p_next);
	region->inuse->p_prev = 0;

	page->p_next = LOG_REGION_ADDR_TO_OFF(region, region->free);
	page->p_prev = 0;
	
	region->free->p_prev = LOG_REGION_ADDR_TO_OFF(region, page);
	region->free = page;

	page->p_num_blk = 0;

	bonsai_clflush(page, sizeof(struct log_page_desc), 1);
}

struct log_page_desc* alloc_log_page(struct log_region *region) {
	struct log_page_desc* page;
	
	page = region->free;
	region->free = (struct log_page_desc*)LOG_REGION_OFF_TO_ADDR(region, page->p_next);
	region->free->p_prev = 0;

	page->p_next = LOG_REGION_ADDR_TO_OFF(region, region->inuse);
	page->p_prev = 0;
	region->inuse->p_prev = LOG_REGION_ADDR_TO_OFF(region, page);
		
	region->inuse = page;

	bonsai_clflush(page, sizeof(struct log_page_desc), 1);

	return page;
}

static inline void init_log_page(struct log_page_desc* page, off_t page_off, int first, int last) {
	page->p_off = page_off;
	page->p_num_blk = 0;
	page->p_prev = first ? (page_off - PAGE_SIZE) : 0;
	page->p_next = last ? (page_off + PAGE_SIZE) : 0;
}

static void init_per_cpu_log_region(struct log_region* region, struct log_region_desc *desc, 
			unsigned long paddr, off_t offset, size_t size) {
	int i, num_page = LOG_REGION_SIZE / NUM_PHYSICAL_CPU_PER_SOCKET / PAGE_SIZE;

	desc->r_off = offset;
	desc->r_size = size;
	desc->r_oplog_top = 0;

	bonsai_clflush(desc, sizeof(struct log_region_desc), 1);

	memset((char*)paddr, 0, size);

	spin_lock_init(&region->lock);
	region->first_blk = NULL;
	region->curr_blk = NULL;
	region->start = paddr;
	region->free = (struct log_page_desc*)paddr;
	region->inuse = NULL;

	for (i = 0; i < num_page; i++, paddr += PAGE_SIZE, offset += PAGE_SIZE) {
		init_log_page((struct log_page_desc*)(paddr), offset, 
						(i == 0) ? 1 : 0,
						(i == num_page - 1) ? 1 : 0);
	}
}

void log_region_deinit(struct log_layer* layer) {
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		pmem_unmap(layer->pmem_addr[node], LOG_REGION_SIZE);
	}
}

int log_region_init(struct log_layer* layer, struct bonsai_desc* bonsai) {
	struct log_region_desc *desc;
	struct log_region *region;
	int node, cpu, is_pmem, error = 0;
	size_t size_per_cpu = LOG_REGION_SIZE / NUM_PHYSICAL_CPU_PER_SOCKET;
	size_t mapped_len;
	char* pmemaddr;

	for (node = 0; node < NUM_SOCKET; node ++) {
		/* create a pmem file */
		pmemaddr = pmem_map_file(log_region_fpath[node], LOG_REGION_SIZE,
						0666, O_CREAT|O_RDWR, &mapped_len, &is_pmem);
		if (pmemaddr == NULL) {
			perror("pmem_map");
			error = -EMMAP;
			goto out;
		}

		layer->pmem_addr[node] = pmemaddr;
		
		for (cpu = 0; cpu < NUM_PHYSICAL_CPU_PER_SOCKET; cpu ++, pmemaddr += size_per_cpu) {
			region = &layer->region[cpu];
			
			desc = &bonsai->log_region[OS_CPU_ID[node][cpu][0]];
			init_per_cpu_log_region(region, desc, (unsigned long)pmemaddr, 
				pmemaddr - layer->pmem_addr[node], size_per_cpu);

			region->desc = desc;
		}
	}

out:
	return error;
}

static inline int file_exists(const char *filename) {
    struct stat buffer;
    return stat(filename, &buffer);
}

int data_region_init(struct data_layer *layer) {
	struct data_region *region;
	int node, sds_write_value = 0;
	PMEMobjpool* pop;

	pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

	for (node = 0; node < NUM_SOCKET; node ++) {
		region = &layer->region[node];
		if (file_exists(data_region_fpath[node]) != 0)
			perror("create a new pmdk pool error:\n");
		
		if ((pop = pmemobj_create(data_region_fpath[node], POBJ_LAYOUT_NAME(bonsai##node),
                              (uint64_t)DATA_REGION_SIZE, 0666)) == NULL) {
			perror("fail to create object pool\n");
			return -EPMEMOBJ;
		}

		region->pop = pop;
	}

	return 0;
}

void data_region_deinit(struct data_layer *layer) {
	struct data_region *region;
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		region = &layer->region[node];
		pmemobj_close(region->pop);
	}
}
