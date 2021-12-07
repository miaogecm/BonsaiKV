#ifndef EPOCH_H
#define EPOCH_H

#ifdef __cplusplus
extern "C" {
#endif

#define EPOCH	1000 /* 1ms */

extern int epoch_init();
extern int thread_epoch_init();

#ifdef __cplusplus
}
#endif

#endif
