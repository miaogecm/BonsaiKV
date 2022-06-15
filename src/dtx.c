/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 * 
 * A durable transaction implementation.
 */
#include "bonsai.h"
#include "dtx.h"
#include "crc.h"

#include <string.h>

extern struct bonsai_info* bonsai;

struct dtx_struct* dtx_begin(uint64_t timestamp) {
	struct dtx_struct* dtx = NULL;

	/* FIXME: dtx memory allocation */
	memset(dtx, 0, sizeof(struct dtx_struct));
	
	dtx->t_status 	= DTX_RUNNING;
	dtx->t_id		= atomic_add_return(1, &bonsai->tx_id);
	dtx->t_version	= timestamp;

	return dtx;
}

void dtx_commit(struct dtx_struct* dtx) {
	
	dtx->t_checksum = crc32(~0, (uint8_t*)dtx->t_start, dtx->t_size);
	dtx->t_status = DTX_COMMIT;

	/* persist the transaction header */
	bonsai_flush(dtx, sizeof(struct dtx_struct) + dtx->t_num * sizeof(logid_t), 1);
}

void dtx_abort() {
	/* TODO */
}
