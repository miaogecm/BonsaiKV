#ifndef REGION_H
#define REGION_H

#ifdef __cplusplus
extern "C" {
#endif

#include <libpmemobj.h>

#include "spinlock.h"
#include "common.h"

#define LOG_REGION_SIZE		1024*1024*32UL /* 32 MB */
#define DATA_REGION_SIZE	1024*1024*32UL /* 32 MB */

struct log_page_desc {
	__le64 p_off; /* page offset */
	__le64 p_num_blk; /* how many log block in this page */
	__le64 p_prev; /* previous log page */ 
	__le64 p_next; /* next log page */ 
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
	PMEMobjpool* pop;
};

#define LOG_PAGE_DESC(addr)	(struct log_page_desc*)((unsigned long)addr & PAGE_MASK)

#define LOG_REGION_ADDR_TO_OFF(region, addr) ((unsigned long)addr - region->start)
#define LOG_REGION_OFF_TO_ADDR(region, off) (region->start + off)

struct log_layer;
struct data_layer;
struct bonsai_desc;

extern int log_region_init(struct log_layer* layer, struct bonsai_desc* desc);
extern void log_region_deinit(struct log_layer* layer);

extern int data_region_init(struct data_layer *layer);
extern void data_region_deinit(struct data_layer *layer);

extern struct oplog_blk* alloc_oplog_block(int cpu);

extern struct log_page_desc* alloc_log_page(struct log_region *region);
extern void free_log_page(struct log_region *region, struct log_page_desc* page);

#ifdef __cplusplus
}
#endif

#endif
