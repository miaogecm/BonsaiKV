#ifndef REGION_H
#define REGION_H

#include "common.h"

#define PAGE_SHIFT	12
#define PAGE_MASK	(1 << 12)

#define LOG_REGION_SIZE	1024*1024*1024*8 /* 8 GB */
#define DATA_REGION_SIZE	1024*1024*1024*8 /* 8 GB */

struct log_page_desc {
	__le64 p_off; /* page offset */
	__le64 p_num_log; /* how many oplogs in this page */
	__le64 p_next; /* next log page */
	char padding[8]; 
};

struct log_region_desc {
	__le64 r_off; /* region offset */
	__le64 r_size; /* region size */	
	__le64 r_oplog_top; /* oplog top */
};

struct log_region {
	spinlock_t lock;
	struct oplog_blk* first_blk;
	struct oplog_blk* curr_blk;

	unsigned long start; /* memory-mapped address */
	struct log_region_desc *desc;
	struct log_page_desc* free;
	struct log_page_desc* inuse;
};

struct data_region {
	PMEMobjpool pop;
};

#define LOG_PAGE_DESC(addr)	(struct log_page_desc*)(addr & PAGE_MASK)

#define LOG_REGION_ADDR_TO_OFF(region, addr) ((unsigned long)addr - region->start)
#define LOG_REGION_OFF_TO_ADDR(region, off) (region->start + off)

extern int log_region_init(struct log_layer* layer, struct bonsai_desc* desc);
extern int log_region_deinit(struct log_layer* layer);

extern int data_region_init(struct data_layer *layer);

extern struct oplog_blk* alloc_oplog_block(int cpu);

#endif
