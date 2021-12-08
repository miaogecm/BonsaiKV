#ifndef EPOCH_H
#define EPOCH_H

#ifdef __cplusplus
extern "C" {
#endif

#define EPOCH	1000 /* 1ms */

extern int epoch_init();

extern int thread_set_alarm();
extern void thread_block_alarm();

#ifdef __cplusplus
}
#endif

#endif
