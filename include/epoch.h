#ifndef EPOCH_H
#define EPOCH_H

#ifdef __cplusplus
extern "C" {
#endif

#define EPOCH	100000 /* 100ms */

extern int epoch_init();
extern void try_run_epoch();

extern int thread_register_alarm_signal();
extern int thread_block_alarm_signal();

#ifdef __cplusplus
}
#endif

#endif
