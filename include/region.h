#ifndef REGION_H
#define REGION_H

#ifdef __cplusplus
extern "C" {
#endif

#include <libpmemobj.h>

#include "spinlock.h"
#include "common.h"
#include "arch.h"

#define LOG_REGION_SIZE		38654705664UL  /* 36 GB, 16777216 logs per CPU */
#define DATA_REGION_SIZE	25769803776UL  /* 24 GB, 805306368 entries */

struct data_region {
	int d_fd; /* file descriptor */
	void* d_start; /* memory-mapped address */
};

#define LOG_PAGE_DESC(addr)	(struct log_page_desc*)((unsigned long)(addr) & PAGE_MASK)

#define LOG_REGION_ADDR_TO_OFF(region, addr) ((unsigned long)(addr) - region->vaddr)
#define LOG_REGION_OFF_TO_ADDR(region, off) (region->vaddr + off)

struct log_layer;
struct data_layer;
struct bonsai_desc;

extern int log_region_init(struct log_layer* layer);
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
