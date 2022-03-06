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
#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <numa.h>

#include "arch.h"
#include "bonsai.h"
#include "region.h"
#include "cpu.h"
#include "pnode.h"
#include "numa_config.h"

static char *log_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/region0",
	"/mnt/ext4/node1/region1"
};

static char* data_region_fpath[NUM_SOCKET] = {
	"/mnt/ext4/node0/objpool0",
	"/mnt/ext4/node1/objpool1"
};

void free_log_page(struct log_region *region, struct log_page_desc* page) {
	struct log_page_desc* prev_page = NULL, *next_page = NULL;

	spin_lock(&region->inuse_lock);
	if (page->p_prev != -PAGE_SIZE)
		prev_page = (struct log_page_desc*)LOG_REGION_OFF_TO_ADDR(region, page->p_prev);
	if (page->p_next != -PAGE_SIZE)
		next_page = (struct log_page_desc*)LOG_REGION_OFF_TO_ADDR(region, page->p_next);

	if (prev_page && next_page) {
		prev_page->p_next = page->p_next;
		next_page->p_prev = page->p_prev;
	} else if (!prev_page && next_page) {
		/* I am the first */
		next_page->p_prev = -PAGE_SIZE;
		region->inuse = next_page;
	} else if (prev_page && !next_page) {
		/* I am the last */
		prev_page->p_next = -PAGE_SIZE;
	} else {
		/* I am the only one */
		region->inuse = NULL;
	}
	spin_unlock(&region->inuse_lock);

	page->p_prev = -PAGE_SIZE;

	spin_lock(&region->free_lock);
	page->p_next = LOG_REGION_ADDR_TO_OFF(region, region->free);
	region->free->p_prev = LOG_REGION_ADDR_TO_OFF(region, page);
	region->free = page;
	spin_unlock(&region->free_lock);

	bonsai_flush((void*)page, sizeof(struct log_page_desc), 1);
}

struct log_page_desc* alloc_log_page(struct log_region *region) {
	struct log_page_desc* page;

	spin_lock(&region->free_lock);
	page = region->free;
    assert(page->p_next != -PAGE_SIZE);
	region->free = (struct log_page_desc*)LOG_REGION_OFF_TO_ADDR(region, page->p_next);
	region->free->p_prev = -PAGE_SIZE;
	spin_unlock(&region->free_lock);

	spin_lock(&region->inuse_lock);
	if (likely(region->inuse)) {
		page->p_next = LOG_REGION_ADDR_TO_OFF(region, region->inuse);
		region->inuse->p_prev = LOG_REGION_ADDR_TO_OFF(region, page);
	} else
		page->p_next = -PAGE_SIZE;
		
	region->inuse = page;
	spin_unlock(&region->inuse_lock);

	bonsai_flush((void*)page, sizeof(struct log_page_desc), 1);

	return page;
}

static inline void init_log_page(struct log_page_desc* page, off_t page_off, int first, int last) {
	page->p_off = page_off;
	page->p_num_blk = 0;
	page->p_prev = first ? -PAGE_SIZE : (page_off - PAGE_SIZE);
	page->p_next = last ? -PAGE_SIZE : (page_off + PAGE_SIZE);
}

static void init_per_cpu_log_region(struct log_region* region, struct log_region_desc *desc, unsigned long start,
			unsigned long vaddr, off_t offset, size_t size) {
	int i, num_page = LOG_REGION_SIZE / NUM_CPU_PER_SOCKET / PAGE_SIZE;

	desc->r_off = offset;
	desc->r_size = size;
	desc->r_oplog_top = 0;

	bonsai_flush((void*)desc, sizeof(struct log_region_desc), 1);

	memset((char*)vaddr, 0, size);

	spin_lock_init(&region->free_lock);
	spin_lock_init(&region->inuse_lock);
	
	region->first_blk = (struct oplog_blk*)(vaddr + sizeof(struct oplog_blk));
	region->curr_blk = NULL;
	region->vaddr = start;
	region->start = vaddr;
	region->free = (struct log_page_desc*)vaddr;
	region->inuse = NULL;

	for (i = 0; i < num_page; i++, vaddr += PAGE_SIZE, offset += PAGE_SIZE) {
		init_log_page((struct log_page_desc*)(vaddr), offset, 
						(i == 0) ? 1 : 0,
						(i == num_page - 1) ? 1 : 0);
	}
}

void log_region_deinit(struct log_layer* layer) {
	int node;

	for (node = 0; node < NUM_SOCKET; node ++) {
		munmap(layer->pmem_addr[node], LOG_REGION_SIZE);
		close(layer->pmem_fd[node]);
	}
}

int log_region_init(struct log_layer* layer, struct bonsai_desc* bonsai) {
	struct log_region_desc *desc;
	struct log_region *region;
	int node, cpu_idx, fd, cpu, error = 0;
	size_t size_per_cpu = LOG_REGION_SIZE / NUM_CPU_PER_SOCKET;
	char *vaddr, *start;

	for (node = 0; node < NUM_SOCKET; node ++) {
		/* create a pmem file */
    	if ((fd = open(log_region_fpath[node], O_CREAT|O_RDWR, 0666)) < 0) {
        	perror("open");
        	goto out;
    	}

    	/* allocate the pmem */
    	if ((error = posix_fallocate(fd, 0, LOG_REGION_SIZE)) != 0) {
        	perror("posix_fallocate");
        	goto out;
    	}

    	/* memory map it */
    	if ((vaddr = mmap(NULL, LOG_REGION_SIZE, 
				PROT_READ|PROT_WRITE, MAP_FILE|MAP_SHARED, fd, 0)) == MAP_FAILED) {
       		perror("mmap");
        	goto out;
    	}
		
		start = vaddr;
		
		bonsai_print("node[%d] log region: [%016lx %016lx]\n", node, vaddr, vaddr + LOG_REGION_SIZE);
		
		layer->pmem_fd[node] = fd;
		layer->pmem_addr[node] = vaddr;
		
		for (cpu_idx = 0; cpu_idx < NUM_CPU_PER_SOCKET; cpu_idx ++, vaddr += size_per_cpu) {
            cpu = node_to_cpu(node, cpu_idx);

			region = &layer->region[cpu];
			desc = &bonsai->log_region[cpu];
		
			init_per_cpu_log_region(region, desc, (unsigned long)start, (unsigned long)vaddr, 
				vaddr - layer->pmem_addr[node], size_per_cpu);

			bonsai_print("init cpu_idx[%d] log region: [%016lx %016lx], size %lu\n",
                         cpu_idx, (unsigned long)vaddr, (unsigned long)vaddr + size_per_cpu, size_per_cpu);

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
	int node;
	PMEMobjpool* pop;

	for (node = 0; node < NUM_SOCKET; node ++) {
		region = &layer->region[node];
		
		if (!file_exists(data_region_fpath[node])) {
			perror("create a existed new pmdk pool");
			return -EPMEMOBJ;
		}
		
		if ((pop = pmemobj_create(data_region_fpath[node], 
								POBJ_LAYOUT_NAME(BONSAI),
                              	DATA_REGION_SIZE, 0666)) == NULL) {
			perror("fail to create object pool");
			return -EPMEMOBJ;
		}
		region->pop = pop;
		region->start = (unsigned long)pop;
		region->nab = numa_alloc_onnode(sizeof(struct nab_blk_table), node);

		bonsai_print("data_region_init node[%d] region: [%016lx, %016lx]\n", 
			node, pop, (unsigned long)pop + DATA_REGION_SIZE);
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
