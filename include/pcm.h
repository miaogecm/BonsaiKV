#ifndef BONSAI_PCM_H
#define BONSAI_PCM_H

#include <stdint.h>

void pcm_on();
void pcm_start();
uint64_t pcm_get_nr_remote_pmem_access_packet();

#endif //BONSAI_PCM_H
