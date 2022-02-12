#include <sys/time.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "btree.h"

#include "../../data/kvdata.h"

static inline int file_exists(char const *file) { return access(file, F_OK); }

static char* persistent_path = "/mnt/ext4/pool";

#define N           1000000
#define NUM_THREAD  4
#define NUM_CPU		  8

entry_key_t a[N];
pthread_t tids[NUM_THREAD];

TOID(btree) bt = TOID_NULL(btree);
PMEMobjpool *pop;

void init_pmdk() {
  if (file_exists(persistent_path) != 0) {
    pop = pmemobj_create(persistent_path, "btree", 1000000000UL,
                         0666); // make 1GB memory pool
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  } else {
    pop = pmemobj_open(persistent_path, "btree");
    bt = POBJ_ROOT(pop, btree);
    printf("pool existed\n");
    exit(1);
  }
}

static inline int get_cpu() {
	cpu_set_t mask;
	int i;

	if (sched_getaffinity(0, sizeof(cpu_set_t), &mask) == -1)
		perror("sched_getaffinity fail\n");

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
        perror("bind cpu failed\n");
    }
}

void* thread_fun(void* arg) {
	long i, id = (long)arg;
	char* v = 0;
	char** val_arr = (char**) malloc(sizeof(char*) * N);

	bind_to_cpu(id);

	for (i = 0; i < N; i ++) {

        switch (op_arr[id][i][0]) {
        case 0:
            D_RW(bt)->btree_insert(op_arr[id][i][1], (char*)op_arr[id][i][2]);
            break;
        case 1:
            D_RW(bt)->btree_insert(op_arr[id][i][1], (char*)op_arr[id][i][2]);
            break;        
        case 2:
            D_RW(bt)->btree_search(op_arr[id][i][1]);
            break;
        case 3:
            D_RW(bt)->btree_search_range(op_arr[id][i][1], op_arr[id][i][2], (unsigned long*)val_arr);
            break;
        default:
            printf("unknown type\n");
            assert(0);
            break;
        }
	}

	// printf("user thread[%ld]---------------------end---------------------\n", id);

	free(val_arr);
    
	// printf("user thread[%ld] exit\n", id);

	return NULL;
}

int main(int argc, char **argv) {
  long i;
  struct timeval t0, t1;
  double interval;

  bind_to_cpu(0);

  init_pmdk();
  printf("start load [%d] entries\n", N);
  gettimeofday(&t0, NULL);
  for (i = 0; i < N; i ++) {
      D_RW(bt)->btree_insert(load_arr[i][0], (char*)load_arr[i][1]);
  }
  gettimeofday(&t1, NULL);
  interval = t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
  printf("load finished in %.3lf seconds\n", interval);

  sleep(5);

  printf("start workload ([%d] ops, [%d] threads)\n", N, NUM_THREAD);
  gettimeofday(&t0, NULL);
	for (i = 0; i < NUM_THREAD; i++) {
		pthread_create(&tids[i], NULL, thread_fun, (void*)i);
	}

	for (i = 0; i < NUM_THREAD; i++) {
		pthread_join(tids[i], NULL);
	}
  gettimeofday(&t1, NULL);
  interval = t1.tv_sec - t0.tv_sec + (t1.tv_usec - t0.tv_usec) / 1e6;
  printf("workload finished in %.3lf seconds\n", interval);

  pmemobj_close(pop);
  printf("end\n");
  return 0;
}
