#ifndef CPU_H
#define CPU_H

#ifdef __cplusplus
extern "C" {
#endif

#include <sched.h>
#include <stdio.h>
#include <assert.h>

#include "config.h"

#define NOCPU	(-1)
#define MAXCPU	NUM_CPU

extern int cpu_used[NUM_CPU];

static inline int get_chip() {
  unsigned long a, d, c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  return (c & 0xFFF000) >> 12;
}

static inline int get_core() {
  unsigned long a, d, c;
  asm volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
  return c & 0xFFF;
}

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

static inline void bind_to_node(int node) {
    cpu_set_t mask;
    int idx, cpu;
    CPU_ZERO(&mask);
    for (idx = 0; idx < NUM_CPU_PER_SOCKET; idx++) {
        cpu = node_idx_to_cpu(node, idx);
        CPU_SET(cpu, &mask);
    }
    if ((sched_setaffinity(0, sizeof(cpu_set_t), &mask)) != 0) {
        perror("bind node failed\n");
    }
}

static inline int get_numa_node(int cpu) {
	return cpu_to_node((cpu != NOCPU) ? cpu : get_cpu());
}

static inline void mark_cpu(int cpu) {
	cpu_used[cpu] = 1;
}

static inline int alloc_cpu_onnode(int node) {
	int cpu_idx, cpu;
	for (cpu_idx = 0; cpu_idx < NUM_CPU_PER_SOCKET; cpu_idx++) {
		cpu = node_idx_to_cpu(node, cpu_idx);
		if (!cpu_used[cpu]) {
			mark_cpu(cpu);
			return cpu;
		}
	}
    return -node_idx_to_cpu(node, 0) - 1;
}

#ifdef __cplusplus
}
#endif

#endif
