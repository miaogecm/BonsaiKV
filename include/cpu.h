#ifndef CPU_H
#define CPU_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sched.h>
#include <stdio.h>

#include "numa_config.h"

#define NOCPU	-1
#define MAXCPU	48

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1)
		perror("sched_getaffinity fail\n");

	for (i = 0; i < MAXCPU; i++) {
		if (CPU_ISSET(i, &mask))
			return i;
	}

	return -1;
}

static inline void bind_to_cpu(int cpu) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpu, &mask);
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind cpu failed\n");
    }
}

static inline int get_numa_node(int cpu) {
	// return ((cpu != NOCPU) ? cpu : get_cpu()) % (NUM_SOCKET + 1);
	return 0;
}

#ifdef __cplusplus
}
#endif

#endif
