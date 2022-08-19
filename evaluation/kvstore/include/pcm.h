#ifndef PCM_H
#define PCM_H

#include <stdint.h>

void pcm_on();
void pcm_start();
uint64_t pcm_get_nr_remote_pmem_access_packet();

#endif //PCM_H
