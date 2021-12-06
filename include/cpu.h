#ifndef CPU_H
#define CPU_H

#ifdef __cplusplus
extern "C" {
#endif

#define _GNU_SOURCE
#include <sched.h>
#include <stdio.h>

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1
		printf("sched_getaffinity fail\n");

	for (i = 0; i < NUM_CPU; i++) {
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
        printf("bind cpu[%d] failed.\n", cpu);
    }
}

static inline int get_numa_node() {
	return 0;
}

#ifdef __cplusplus
}
#endif

#endif
