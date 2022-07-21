/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
#include "cpu.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>

#include "bonsai.h"
#include "log_layer.h"
#include "ordo.h"
#include "arch.h"
#include "region.h"
#include "common.h"
#include "thread.h"
#include "data_layer.h"
#include "index_layer.h"
#include "rcu.h"

//#define LCB_FULL_SIZE   3072
#define LCB_FULL_SIZE	1536
#define LCB_MAX_SIZE    (LCB_FULL_SIZE * 2)

#define LCB_FULL_NR     (LCB_FULL_SIZE / sizeof(struct oplog))
#define LCB_MAX_NR      (LCB_MAX_SIZE / sizeof(struct oplog))

#define OPLOG_FLIP(t)   ((t) & 1)
#define OPLOG_TYPE(t)   ((t) & 6)
#define OPLOG_TXOP(t)     ((t) & 24)

#define WB_SIGNO        SIGUSR1

#define CHECK_NLOG_INTERVAL     20

union logid_u {
    logid_t id;
    struct {
        uint32_t cpu : 7;
        uint32_t off : 25;
    };
};

typedef struct {
    struct oplog *logs;
    int cnt;
} logs_t;

struct cpu_log_snapshot {
    uint32_t region_start, region_end;
    int cpu;
};

struct flush_load {
    int color;
    size_t load;
    struct list_head cluster;
};

struct cluster {
    pnoid_t pnode;
    struct list_head pbatch_list;
    struct list_head list;
};

/* pflush work descriptor (be passed to each worker's arg) */
struct pflush_work_desc {
    int wid, cpu;
    void *workset;
};

/* workset definitions */

struct desc_workset {
    struct pflush_work_desc desc[NUM_PFLUSH_WORKER];
};

struct fetch_workset {
    int fetch_task[NUM_PFLUSH_WORKER][NUM_CPU];

    /* The output of the fetch work. Collected logs for each worker. */
    logs_t per_worker_logs[NUM_PFLUSH_WORKER];

    uint32_t new_region_starts[NUM_CPU];
};

struct sort_contrib {
    int cnt;
    struct oplog *start;
};

struct clustering_workset {
    /* The input of the cluster work. Collected logs for each worker. */
    logs_t *per_worker_logs;

    pbatch_op_t *pbatch_ops[NUM_PFLUSH_WORKER][NUM_SOCKET];

    struct oplog *local_samples[NUM_PFLUSH_WORKER];
    int local_sample_cnt[NUM_PFLUSH_WORKER];
    struct oplog global_samples[NUM_PFLUSH_WORKER];

    struct sort_contrib sort_contribs[NUM_PFLUSH_WORKER][NUM_PFLUSH_WORKER];

    struct flush_load per_worker_per_socket_loads[NUM_PFLUSH_WORKER][NUM_SOCKET];

    /* The output of the clustering work. Loads for each NUMA node. */
    struct flush_load per_socket_loads[NUM_SOCKET];

    pthread_barrier_t barrier;
};

struct load_balance_workset {
    /* The input of the load balance work. Loads for each NUMA node. */
    struct flush_load *per_socket_loads;

    /* The output of the load balance work. Loads for each worker. */
    struct flush_load per_worker_loads[NUM_PFLUSH_WORKER];

    pthread_barrier_t barrier;
};

struct flush_workset {
    /* The input of the flush work. Loads for each worker. */
    struct flush_load *per_worker_loads;
    void *shim_recycle_chains[NUM_PFLUSH_WORKER];
};

struct pflush_worksets {
    struct desc_workset         desc_ws;
    struct fetch_workset        fetch_ws;
    struct clustering_workset   clustering_ws;
    struct load_balance_workset load_balance_ws;
    struct flush_workset        flush_ws;
};

static void check_flush_load(struct flush_load *load) {
    struct cluster *cluster;
    pbatch_cursor_t cursor;
    pbatch_op_t *op;
    list_for_each_entry(cluster, &load->cluster, list) {
        pbatch_cursor_init(&cursor, &cluster->pbatch_list);
        while (!pbatch_cursor_is_end(&cursor)) {
            op = pbatch_cursor_get(&cursor);
            assert(pkey_compare(op->key, pnode_get_lfence(cluster->pnode)) >= 0);
            assert(pkey_compare(op->key, pnode_get_rfence(cluster->pnode)) < 0);
            pbatch_cursor_inc(&cursor);
        }
    }
}

static void cluster_dump(struct list_head* list) {
    struct cluster* cl;
    printf("----------cluster dump start----------\n");
    list_for_each_entry(cl, list, list) {
        printf("pnode id: %u\n", cl->pnode);
        pbatch_list_dump(&cl->pbatch_list);
    }
    printf("----------cluster dump end----------\n");
}

static void flush_load_dump(struct flush_load* load) {
    printf("----------flush load dump start----------\n");
    printf("load size: %lu\n", load->load);
    cluster_dump(&load->cluster);
    printf("----------flush load dump end----------\n");
}

static void flush_load_destroy(struct flush_load *load) {
    struct cluster *cl, *tmp;
    list_for_each_entry_safe(cl, tmp, &load->cluster, list) {
        pbatch_list_destroy(&cl->pbatch_list);
        free(cl);
    }
}

static void flush_load_move(struct flush_load *dst, struct flush_load *src) {
    memcpy(dst, src, sizeof(*src));
    list_replace(&src->cluster, &dst->cluster);
}

static inline size_t max_load_per_socket(size_t avg_load) {
    return avg_load + avg_load / 8;
}

static inline int oplog_overflow_size(struct cpu_log_region_meta *meta, uint32_t off) {
    uint32_t d1 = (meta->end - meta->start + NUM_OPLOG_PER_CPU) % NUM_OPLOG_PER_CPU;
    uint32_t d2 = (off - meta->start + NUM_OPLOG_PER_CPU) % NUM_OPLOG_PER_CPU;
    return (int) (d2 - d1);
}

void oplog_snapshot_lst(log_state_t *lst) {
    *lst = LOG(bonsai)->lst;
}

/* Get oplog address by logid. */
struct oplog *oplog_get(logid_t logid) {
    union logid_u id = { .id = logid };
    struct cpu_log_region_desc *desc;
    struct cpu_log_region_meta meta;
    struct oplog *oplog;
    unsigned seq;
    int overflow;

    desc = &LOG(bonsai)->desc->descs[id.cpu];

    do {
        seq = read_seqcount_begin(&desc->seq);

        meta = ACCESS_ONCE(desc->region->meta);
        overflow = oplog_overflow_size(&meta, id.off);

        if (unlikely(overflow >= 0)) {
            oplog = &desc->lcb[overflow];
        } else {
            oplog = &desc->region->logs[id.off];
        }
    } while (read_seqcount_retry(&desc->seq, seq));

    return oplog;
}

static void write_back(int cpu, int dimm_unlock, void *wb_new_buf_in_signal) {
	struct log_layer* layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;
    uint32_t end = region->meta.end;
    struct oplog *lcb;
    size_t len, c;

    /* Make sure value allocations are persistent. */
    valman_persist_cpu(cpu);

    lcb = local_desc->lcb;
    len = local_desc->size;

    while ((c = min(NUM_OPLOG_PER_CPU- end, len))) {
        memcpy_nt(&region->logs[end], lcb, c * sizeof(struct oplog), 0);
        end  = (end + c) % NUM_OPLOG_PER_CPU;
        len -= c;
        lcb += c;
    }
    memory_sfence();

    if (dimm_unlock) {
        pthread_mutex_unlock(local_desc->dimm_lock);
    }

    if (!wb_new_buf_in_signal) {
        /* Not in signal. */
        lcb = malloc(LCB_MAX_SIZE * sizeof(*lcb));
        call_rcu(RCU(bonsai), free, local_desc->lcb);
    } else {
        lcb = wb_new_buf_in_signal;
        /* TODO: Delay-free local_desc->lcb (@call_rcu is non-reentrant, so we can not use it here.) */
    }

    local_desc->size = 0;

    write_seqcount_begin(&local_desc->seq);
    region->meta.end = end;
    local_desc->lcb = lcb;
    write_seqcount_end(&local_desc->seq);

    bonsai_flush(&region->meta.end, sizeof(__le32), 1);
}

static void handle_wb(int signo) {
    int cpu = __this->t_cpu;
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];

    switch (local_desc->wb_state) {
        case WBS_ENABLE:
            write_back(cpu, 0, local_desc->wb_new_buf);
            local_desc->wb_done = 1;
            break;

        case WBS_DELAY:
            local_desc->wb_state = WBS_REQUEST;
            break;

        default:
            assert(0);
    }
}

logid_t oplog_insert(log_state_t *lst, pkey_t key, pval_t val, optype_t op, txop_t txop, int cpu) {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;
    uint32_t end = region->meta.end;
    static __thread int last = 0;
	struct oplog* log;
    union logid_u id;

    local_desc->wb_state = WBS_DELAY;
    barrier();

    assert(local_desc->size < LCB_MAX_NR);

    id.cpu = cpu;
    id.off = (local_desc->size + end) % NUM_OPLOG_PER_CPU;

    log = &local_desc->lcb[local_desc->size++];
	log->o_type = cpu_to_le64(txop | op | lst->flip);
    log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_kv.k = key;
	log->o_kv.v = val;

    if (unlikely(local_desc->size >= LCB_FULL_NR
			&& local_desc->size % 4 == 0)) {
        if (pthread_mutex_trylock(local_desc->dimm_lock)) {
            write_back(cpu, 1, NULL);
        } else if (unlikely(local_desc->size >= LCB_MAX_NR)) {
            write_back(cpu, 0, NULL);
        }
    }

    if (cmpxchg_local(&local_desc->wb_state, WBS_DELAY, WBS_ENABLE) == WBS_REQUEST) {
        write_back(cpu, 0, NULL);
        local_desc->wb_state = WBS_ENABLE;
        local_desc->wb_done = 1;
    }

    if (++last == CHECK_NLOG_INTERVAL) {
        if (atomic_add_return(CHECK_NLOG_INTERVAL, &layer->nlogs[cpu].cnt) >= CHKPT_NLOG_INTERVAL / NUM_CPU &&
            !atomic_read(&layer->checkpoint)) {
            wakeup_master();
        }
        last = 0;
    }

    return id.id;
}

static inline int worker_id(int numa_node, int nr) {
    return NUM_PFLUSH_WORKER_PER_NODE * numa_node + nr;
}

static inline int worker_numa_node(int wid) {
    return wid / NUM_PFLUSH_WORKER_PER_NODE;
}

static inline int worker_nr(int wid) {
    return wid % NUM_PFLUSH_WORKER_PER_NODE;
}

static inline struct thread_info *worker_thread_info(int wid) {
    return bonsai->pflush_workers[worker_numa_node(wid)][worker_nr(wid)];
}

static inline int worker_cpu(int wid) {
    return (int) worker_thread_info(wid)->t_cpu;
}

static inline void new_flip() {
    rcu_t *rcu = RCU(bonsai);

    LOG(bonsai)->lst.flip ^= 1;
	
    /* Wait for all the user threads to observe the latest flip. */
    rcu_synchronize(rcu, rcu_now(rcu));
}

static void launch_workers(struct pflush_worksets *worksets, work_func_t fn, void *workset) {
    struct work_struct *work;
    struct pflush_work_desc *desc;
    int wid;

    for (wid = 0; wid < NUM_PFLUSH_WORKER; wid++) {
        desc = &worksets->desc_ws.desc[wid];
        desc->workset = workset;

        work = malloc(sizeof(struct work_struct));
        INIT_LIST_HEAD(&work->list);
        work->exec = fn;
        work->exec_arg = desc;
        workqueue_add(&bonsai->pflush_workers[worker_numa_node(wid)][worker_nr(wid)]->t_wq, work);
    }

	wakeup_workers();
	park_master();
}

static size_t log_nr(struct cpu_log_snapshot *snap) {
    return (snap->region_end - snap->region_start + NUM_OPLOG_PER_CPU) % NUM_OPLOG_PER_CPU;
}

static void force_wb() {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *desc;
    struct thread_info *ti;
    int i, ret = 0;

	for (i = 0; i < NUM_USER_THREAD; i++) {
		ti = bonsai->user_threads[i];

		desc = &layer->desc->descs[ti->t_cpu];
		desc->wb_done = 0;
        desc->wb_new_buf = malloc(LCB_MAX_SIZE * sizeof(struct oplog));

		ret = pthread_kill(bonsai->tids[ti->t_id], WB_SIGNO);
		if (unlikely(ret)) {
            assert(0);
        }
	}

    for (i = 0; i < NUM_USER_THREAD; i++) {
        ti = bonsai->user_threads[i];

        desc = &layer->desc->descs[ti->t_cpu];

        while (!ACCESS_ONCE(desc->wb_done)) {
            usleep(1000);
        }
    }
}

static void snapshot_cpu_log(struct cpu_log_snapshot *snap, int cpu) {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;

    snap->cpu = cpu;
    snap->region_start = region->meta.start;
    snap->region_end = region->meta.end;
}

static size_t fetch_cpu_logs(size_t *nr_logs_processed, uint32_t *new_region_starts,
                             struct oplog *logs, struct cpu_log_snapshot *snap) {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[snap->cpu];
    struct cpu_log_region *region = local_desc->region;
    struct oplog *plog, *log, *last_commit = logs;
    int target_flip = !layer->lst.flip;
    uint32_t cur, end;

    cur = snap->region_start;
    end = snap->region_end;

    *nr_logs_processed = 0;

    for (log = logs; cur != end; cur = (cur + 1) % NUM_OPLOG_PER_CPU, (*nr_logs_processed)++) {
        plog = &region->logs[cur];
		if (unlikely((int) OPLOG_FLIP(plog->o_type) != target_flip)) {
            break;
        }
        if (OPLOG_TYPE(plog->o_type) != OP_NOP) {
            memcpy(log++, plog, sizeof(*log));
        }
        if (OPLOG_TXOP(plog->o_type) == TX_COMMIT) {
            last_commit = log;
        }
    }

    log = last_commit;

    new_region_starts[snap->cpu] = cur;

    return log - logs;
}

static logs_t fetch_logs(uint32_t *new_region_starts, int nr_cpu, int *cpus) {
    struct cpu_log_snapshot snapshots[NUM_CPU];
    size_t nr_logs_fetched, nr_logs_processed;
    struct log_layer *layer = LOG(bonsai);
    int tot_max = 0, nr_logs_max, i;
    struct oplog *log;
    logs_t fetched;

    for (i = 0; i < nr_cpu; i++) {
        snapshot_cpu_log(&snapshots[i], cpus[i]);
        nr_logs_max = (int) log_nr(&snapshots[i]);
        tot_max += nr_logs_max;
	}

	if (unlikely(!tot_max)) {
        usleep(100);
    }

    log = fetched.logs = malloc(sizeof(*log) * tot_max);

    for (i = 0; i < nr_cpu; i++) {
        nr_logs_fetched = fetch_cpu_logs(&nr_logs_processed, new_region_starts, log, &snapshots[i]);
        log += nr_logs_fetched;
        atomic_sub((int) nr_logs_fetched, &layer->nlogs[cpus[i]].cnt);
    }

    fetched.cnt = (int) (log - fetched.logs);

    return fetched;
}

static void fetch_task_alloc(int fetch_task[][NUM_CPU]) {
    int nr_active_cpu[NUM_SOCKET] = { 0 }, active_cpus[NUM_SOCKET][NUM_CPU_PER_SOCKET], cpu, node;
    int i, j, k, nr_cpu_per_socket, nr_remain;
    struct log_layer *layer = LOG(bonsai);

    memset(fetch_task, -1, sizeof(int) * NUM_CPU * NUM_PFLUSH_WORKER);

    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        if (atomic_read(&layer->nlogs[cpu].cnt) > 0) {
            node = cpu_to_node(cpu);
            active_cpus[node][nr_active_cpu[node]++] = cpu;
        }
    }

    for (node = 0; node < NUM_SOCKET; node++) {
        nr_cpu_per_socket = nr_active_cpu[node] / NUM_PFLUSH_WORKER_PER_NODE;
        nr_remain = nr_active_cpu[node] % NUM_PFLUSH_WORKER_PER_NODE;
        k = 0;
        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            for (j = 0; j < nr_cpu_per_socket + (i < nr_remain ? 1 : 0); j++) {
                fetch_task[worker_id(node, i)][j] = active_cpus[node][k++];
            }
        }
    }
}

/*
 * Fetch worker
 * O: per_worker_logs
 */
static int fetch_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct fetch_workset *ws = desc->workset;
    int wid = desc->wid, nr_cpu;

    flockfile(stdout);
    bonsai_print("fetch worker [%d]: ", wid);
    for (nr_cpu = 0; nr_cpu < NUM_CPU && ws->fetch_task[wid][nr_cpu] != -1; nr_cpu++) {
        bonsai_print("%d ", ws->fetch_task[wid][nr_cpu]);
    }
    bonsai_print("\n");
    funlockfile(stdout);

    ws->per_worker_logs[wid] = fetch_logs(ws->new_region_starts, nr_cpu, ws->fetch_task[wid]);

    return 0;
}

static int lower_bound(struct oplog haystack[], int nr, struct oplog *needle) {
    int mid, l = 0, r = nr;

    while (l < r) {
        mid = l + (r - l) / 2;

        if (oplog_cmp(needle, &haystack[mid]) <= 0) {
            r = mid;
        } else {
            l = mid + 1;
        }
    }

    if (l < nr && oplog_cmp(&haystack[l], needle) < 0) {
        l++;
    }

    return l;
}

void sort_oplogs(struct oplog *oplogs, int nr);

static void collaboratively_sort_logs(struct clustering_workset *ws,
                                      logs_t *logs, pthread_barrier_t *barrier, int wid) {
    struct oplog collected_samples[NUM_PFLUSH_WORKER * NUM_PFLUSH_WORKER], *local_sample, *cur_sample;
    struct oplog *data = logs[wid].logs, *cur, *start, *p, *merged;
    int nr_collected_sample, nr_global_sample, remain;
    int cnt = logs[wid].cnt, nr_local_sample, i, j;
    int total, curp[NUM_PFLUSH_WORKER], min_idx;
    struct sort_contrib *cb;

    /* local sort */
    sort_oplogs(data, cnt);

    /* local sampling */
    nr_local_sample = min(NUM_PFLUSH_WORKER, cnt);
    local_sample = malloc(sizeof(struct oplog) * nr_local_sample);
    for (i = 0, cur = data; i < nr_local_sample; i++, cur += cnt / nr_local_sample) {
        local_sample[i] = *cur;
    }
    ws->local_samples[wid] = local_sample;
    ws->local_sample_cnt[wid] = nr_local_sample;

    pthread_barrier_wait(barrier);

    /* global sampling */
    if (wid == 0) {
        /* collect all local samples */
        nr_collected_sample = 0;
        for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
            for (j = 0; j < ws->local_sample_cnt[i]; j++) {
                collected_samples[nr_collected_sample++] = ws->local_samples[i][j];
            }
        }

        /* sort these local samples */
        sort_oplogs(collected_samples, nr_collected_sample);

        /* sample globally */
        nr_global_sample = min(nr_collected_sample, NUM_PFLUSH_WORKER);
        for (i = 0, cur_sample = collected_samples;
             i < nr_global_sample;
             i++, cur_sample += nr_collected_sample / nr_global_sample) {
            ws->global_samples[i] = *cur_sample;
        }
        for (; i < NUM_PFLUSH_WORKER; i++) {
            ws->global_samples[i].o_kv.k = MAX_KEY;
        }
    }

    pthread_barrier_wait(barrier);

    /* local contribution calculation */
    start = data;
    remain = cnt;
    for (i = 1; i < NUM_PFLUSH_WORKER; i++) {
        cb = &ws->sort_contribs[wid][i - 1];
        cb->start = start;
        cb->cnt = lower_bound(start, remain, &ws->global_samples[i]);
        start += cb->cnt;
        remain -= cb->cnt;
    }
    cb = &ws->sort_contribs[wid][NUM_PFLUSH_WORKER - 1];
    cb->start = start;
    cb->cnt = remain;

    pthread_barrier_wait(barrier);

    /* local merge */
    total = 0;
    for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
        total += ws->sort_contribs[i][wid].cnt;
        curp[i] = 0;
    }
    p = merged = malloc(total * sizeof(*merged));
    for (i = 0; i < total; i++) {
        min_idx = -1;
        for (j = 0; j < NUM_PFLUSH_WORKER; j++) {
            if (curp[j] < ws->sort_contribs[j][wid].cnt && (min_idx == -1 || oplog_cmp(&ws->sort_contribs[j][wid].start[curp[j]], &ws->sort_contribs[min_idx][wid].start[curp[min_idx]]) < 0)) {
                min_idx = j;
            }
        }
        *p++ = ws->sort_contribs[min_idx][wid].start[curp[min_idx]];
        curp[min_idx]++;
    }

    /* save result */
    logs[wid].logs = merged;
    logs[wid].cnt = total;

    pthread_barrier_wait(barrier);

    /* cleanup */
    free(data);

	bonsai_print("[pflush worker %d] collaboratively_sort_logs: got %d logs\n", wid, logs[wid].cnt);
}

static void pbatch_op_add(struct flush_load *per_socket_loads,
                          pbatch_cursor_t *cursors, int *numa_node, struct oplog *log) {
    struct list_head *cluster_list;
    struct cluster *cluster;
    pbatch_cursor_t *cursor;
    pbatch_op_t *op;
    pnoid_t pnode;
    int split = 0;

    /* Are we still in the right pnode? */
    cluster_list = &per_socket_loads[*numa_node].cluster;
    cluster = list_last_entry(cluster_list, struct cluster, list);
    if (list_empty(cluster_list) || !is_in_pnode(cluster->pnode, log->o_kv.k)) {
        pnode = shim_pnode_of(log->o_kv.k);
        *numa_node = pnode_numa_node(pnode);
        split = 1;
    }

    cursor = &cursors[*numa_node];

    /* Insert into current cursor position. */
    op = pbatch_cursor_get(cursor);
    switch (OPLOG_TYPE(log->o_type)) {
        case OP_INSERT: op->type = PBO_INSERT; break;
        case OP_REMOVE: op->type = PBO_REMOVE; break;
        default: assert(0);
    }
    op->done = 0;
    op->key = log->o_kv.k;
    op->val = log->o_kv.v;

    /* Handle split. */
    if (unlikely(split)) {
        cluster = malloc(sizeof(*cluster));
        cluster->pnode = pnode;
        INIT_LIST_HEAD(&cluster->pbatch_list);
        INIT_LIST_HEAD(&cluster->list);
        list_add_tail(&cluster->list, &per_socket_loads[*numa_node].cluster);
		pbatch_list_split(&cluster->pbatch_list, cursor);
    }

    /* Forward the cursor. */
   	pbatch_cursor_inc(cursor);

    per_socket_loads[*numa_node].load++;
}

static void merge_and_cluster_logs(pbatch_op_t **per_socket_pbatch_ops,
                                   struct flush_load *per_socket_loads, logs_t *logs,
                                   pthread_barrier_t *barrier, int wid) {
    int i, next_wid, total = logs[wid].cnt, numa_node = 0;
    struct list_head pbatch_lists[NUM_SOCKET];
    pbatch_cursor_t cursors[NUM_SOCKET];
    struct oplog *log = logs[wid].logs;
    pbatch_op_t *ops;

    for (i = 0; i < NUM_SOCKET; i++) {
        INIT_LIST_HEAD(&per_socket_loads[i].cluster);
        per_socket_loads[i].load = 0;
    }

    if (unlikely(!total)) {
        for (i = 0; i < NUM_SOCKET; i++) {
            per_socket_pbatch_ops[i] = NULL;
        }
		goto out;
    }

    for (i = 0; i < NUM_SOCKET; i++) {
        per_socket_pbatch_ops[i] = ops = malloc(total * sizeof(*ops));
        INIT_LIST_HEAD(&pbatch_lists[i]);
        pbatch_list_create(&pbatch_lists[i], ops, total);
        pbatch_cursor_init(&cursors[i], &pbatch_lists[i]);
    }

    for (; --total; log++) {
        if (pkey_compare(log[0].o_kv.k, log[1].o_kv.k)) {
			/* merge logs */
            pbatch_op_add(per_socket_loads, cursors, &numa_node, log);
        }
    }

    for (next_wid = wid + 1; next_wid < NUM_PFLUSH_WORKER && !logs[next_wid].cnt; next_wid++);
	
    if (next_wid >= NUM_PFLUSH_WORKER || pkey_compare(logs[next_wid].logs[0].o_kv.k, log->o_kv.k)) {
        pbatch_op_add(per_socket_loads, cursors, &numa_node, log);
    }

    /* Remove the trailing empty space. */
    for (i = 0; i < NUM_SOCKET; i++) {
        if (!pbatch_cursor_is_end(&cursors[i])) {
            pbatch_list_split(&pbatch_lists[i], &cursors[i]);
            pbatch_list_destroy(&pbatch_lists[i]);
        }
    }

out:
	pthread_barrier_wait(barrier);

	free(logs[wid].logs);

    for (numa_node = 0; numa_node < NUM_SOCKET; numa_node++) {
        bonsai_print("[pflush worker %d] merge_and_cluster_logs: node %d, %lu logs\n",
                     wid, numa_node, per_socket_loads[numa_node].load);
    }
}

static void inter_worker_cluster(struct flush_load *per_socket_loads,
                                    struct flush_load per_worker_per_socket_loads[][NUM_SOCKET],
                                    pthread_barrier_t *barrier, int wid) {
    struct flush_load *load, *load_per_worker;
    struct list_head *addon, *target;
    struct cluster *prev, *curr;
    int numa_node, i;

    /* Wait for all local cluster to be done. */
    pthread_barrier_wait(barrier);

    /* Only one worker per NUMA node. */
    if (worker_nr(wid) != 0) {
        return;
    }

    numa_node = worker_numa_node(wid);

    load = &per_socket_loads[numa_node];

    load->load = 0;
    load->color = numa_node;
    target = &load->cluster;
    INIT_LIST_HEAD(target);

    for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
        load_per_worker = &per_worker_per_socket_loads[i][numa_node];

        addon = &load_per_worker->cluster;

        if (likely(!list_empty(target) && !list_empty(addon))) {
            prev = list_last_entry(target, struct cluster, list);
            curr = list_first_entry(addon, struct cluster, list);
            if (prev->pnode == curr->pnode) {
                pbatch_list_merge(&prev->pbatch_list, &curr->pbatch_list);
                list_del(&curr->list);
                free(curr);
            }
        }

        list_splice_tail(addon, target);

        load->load += load_per_worker->load;
    }

    bonsai_print("[pflush worker %d] inter_worker_cluster: node %d, %lu logs\n", wid, numa_node, load->load);
}

/*
 * Cluster worker
 * I: per_worker_logs
 * O: per_socket_loads
 */
static int cluster_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct clustering_workset *ws = desc->workset;
    int wid = desc->wid;

    /* Sort them collaboratively. */
    collaboratively_sort_logs(ws, ws->per_worker_logs, &ws->barrier, wid);
    
    /* Merge and cluster the per_worker_logs. (per_worker_logs -> per-worker per-socket loads) */
    merge_and_cluster_logs(ws->pbatch_ops[wid],
                           ws->per_worker_per_socket_loads[wid], ws->per_worker_logs, &ws->barrier, wid);
    // flush_load_dump(&ws->per_worker_per_socket_loads[wid][0]);

    /* Do global inter-worker cluster. (per-worker per-socket loads -> per-socket loads) */
    inter_worker_cluster(ws->per_socket_loads, ws->per_worker_per_socket_loads, &ws->barrier, wid);
    // flush_load_dump(&ws->per_socket_loads[0]);

    return 0;
}

static int cluster_cmp(void *priv, struct list_head *a, struct list_head *b) {
    struct cluster *p = list_entry(a, struct cluster, list), *q = list_entry(b, struct cluster, list);
    size_t na = pbatch_list_len(&p->pbatch_list), nb = pbatch_list_len(&q->pbatch_list);
    if (na > nb) {
        return -1;
    }
    if (na < nb) {
        return 1;
    }
    return 0;
}

static void load_split_and_recolor(struct flush_load *dst, struct flush_load *src, size_t nr, int color) {
    struct cluster *c, *sibling;
    pbatch_cursor_t cursor;
    size_t cnt = 0, len;
    pkey_t *cut;

    list_for_each_entry(c, &src->cluster, list) {
        len = pbatch_list_len(&c->pbatch_list);
        if (cnt + len > nr) {
            break;
        }

        if (color != src->color) {
            pnode_split_and_recolor(&c->pnode, NULL, NULL, color, color);
        }

        cnt += len;
    }

    if (cnt < nr) {
        sibling = malloc(sizeof(*sibling));
        INIT_LIST_HEAD(&sibling->pbatch_list);
        INIT_LIST_HEAD(&sibling->list);

        pbatch_cursor_init(&cursor, &c->pbatch_list);
        pbatch_cursor_forward(&cursor, nr - cnt);

        cut = &pbatch_cursor_get(&cursor)->key;
        pnode_split_and_recolor(&c->pnode, &sibling->pnode, cut, color, src->color);

        pbatch_list_split(&sibling->pbatch_list, &cursor);

        list_add(&sibling->list, &c->list);

        c = sibling;
    }

    INIT_LIST_HEAD(&dst->cluster);
    list_cut_position(&dst->cluster, &src->cluster, c->list.prev);

    dst->load = nr;
    src->load -= nr;

    dst->color = color;
}

static void load_merge(struct flush_load *dst, struct flush_load *src) {
    assert(src->color == dst->color);
    list_splice_tail(&src->cluster, &dst->cluster);
    dst->load += src->load;
    src->load = 0;
}

static void do_inter_socket_load_balance(struct flush_load *per_socket_loads) {
    size_t avg_load = 0, max_load;
    struct flush_load *load, cut;
    int node, victim;

    for (node = 0; node < NUM_SOCKET; node++) {
        avg_load += per_socket_loads[node].load;
    }
    avg_load /= NUM_SOCKET;

    max_load = max_load_per_socket(avg_load);

    for (node = 0; node < NUM_SOCKET; node++) {
        load = &per_socket_loads[node];

        if (load->load > max_load) {
            for (victim = 0; victim < NUM_SOCKET && per_socket_loads[victim].load >= avg_load; victim++);
            assert(victim < NUM_SOCKET);

            list_sort(NULL, &load->cluster, cluster_cmp);
            load_split_and_recolor(&cut, load, load->load - avg_load, victim);
            load_merge(&per_socket_loads[victim], &cut);

            node = -1;
        }
    }
}

static void do_intra_socket_load_balance(struct flush_load *per_worker_loads, struct flush_load *loads,
                                         int nr_worker) {
    int nr_busy_worker = (int) (loads->load % nr_worker), i;
    size_t per_worker_load = loads->load / nr_worker, load;
    for (i = 0; i < nr_worker; i++) {
        load = i < nr_busy_worker ? per_worker_load + 1 : per_worker_load;
        load_split_and_recolor(&per_worker_loads[i], loads, load, loads->color);
    }
}

static void inter_socket_load_balance(struct flush_load *per_socket_loads, int wid) {
    int numa_node;

    if (wid != 0) {
        /* We only want one global worker to do this now. */
        return;
    }

    do_inter_socket_load_balance(per_socket_loads);

    for (numa_node = 0; numa_node < NUM_SOCKET; numa_node++) {
        bonsai_print("[pflush worker %d] inter_socket_load_balance: node %d, %lu logs\n",
                     wid, numa_node, per_socket_loads[numa_node].load);
    }
}

static void intra_socket_load_balance(struct flush_load *per_worker_loads, struct flush_load *per_socket_loads,
                                      pthread_barrier_t *barrier, int wid) {
    struct flush_load per_socket_per_worker_loads[NUM_PFLUSH_WORKER_PER_NODE];
    int numa_node, i;

    /* Wait for inter socket load balance to be done. */
    pthread_barrier_wait(barrier);

    /* Only one worker per NUMA node. */
    if (worker_nr(wid) != 0) {
        return;
    }

    numa_node = worker_numa_node(wid);

    do_intra_socket_load_balance(per_socket_per_worker_loads, &per_socket_loads[numa_node],
                                 NUM_PFLUSH_WORKER_PER_NODE);

    for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
        flush_load_move(&per_worker_loads[worker_id(numa_node, i)], &per_socket_per_worker_loads[i]);
    }

    for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
        bonsai_print("[pflush worker %d] intra_socket_load_balance: worker %d [node %d, #%d], %lu logs\n",
                     wid, worker_id(numa_node, i), numa_node, i, per_worker_loads[worker_id(numa_node, i)].load);
    }
}

/*
 * Load balance worker
 * I: per_socket_loads
 * O: per_worker_loads
 */
static int load_balance_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct load_balance_workset *ws = desc->workset;
    int wid = desc->wid;

    inter_socket_load_balance(ws->per_socket_loads, wid);
    //flush_load_dump(&ws->per_socket_loads[0]);

    intra_socket_load_balance(ws->per_worker_loads, ws->per_socket_loads, &ws->barrier, wid);
    //flush_load_dump(&ws->per_worker_loads[0]);

    return 0;
}

static int flush_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct flush_workset *ws = desc->workset;
    struct flush_load *load = &ws->per_worker_loads[desc->wid];
    log_state_t *lst = &LOG(bonsai)->lst;
    struct cluster *c, *tmp;

    ws->shim_recycle_chains[desc->wid] = shim_create_recycle_chain();

    list_for_each_entry_safe(c, tmp, &load->cluster, list) {
        // pbatch_list_dump(&c->pbatch_list);
        pnode_run_batch(lst, c->pnode, &c->pbatch_list, ws->shim_recycle_chains[desc->wid]);
        // pbatch_list_dump(&c->pbatch_list);

        pbatch_list_destroy(&c->pbatch_list);
        list_del(&c->list);
        free(c);
    }

    return 0;
}

static void cleanup_logs(const uint32_t *new_region_starts) {
	struct log_layer *l_layer = LOG(bonsai);
    int cpu;
    for (cpu = 0; cpu < NUM_CPU; cpu++) {
        l_layer->desc->descs[cpu].region->meta.start = new_region_starts[cpu];
    }
}

static void init_stage(struct pflush_worksets *worksets) {
	struct pflush_work_desc* desc;
	struct thread_info* thread;
	int i, t = 0;

	for (i = 0; i < NUM_PFLUSH_WORKER; i ++) {
		thread = bonsai->pflush_workers[worker_numa_node(i)][worker_nr(i)];
		desc = &worksets->desc_ws.desc[t];
		desc->wid = t++;
		desc->cpu = thread->t_cpu;
	}
	
    pthread_barrier_init(&worksets->clustering_ws.barrier, NULL, NUM_PFLUSH_WORKER);
    pthread_barrier_init(&worksets->load_balance_ws.barrier, NULL, NUM_PFLUSH_WORKER);
}

static void fetch_stage(struct pflush_worksets *worksets, logs_t **per_worker_logs) {
    fetch_task_alloc(worksets->fetch_ws.fetch_task);
    launch_workers(worksets, fetch_work, &worksets->fetch_ws);
    *per_worker_logs = worksets->fetch_ws.per_worker_logs;
}

static void cluster_stage(struct pflush_worksets *worksets,
                             struct flush_load **per_socket_loads, logs_t *per_worker_logs) {
    worksets->clustering_ws.per_worker_logs = per_worker_logs;
    launch_workers(worksets, cluster_work, &worksets->clustering_ws);
    *per_socket_loads = worksets->clustering_ws.per_socket_loads;
}

static void load_balance_stage(struct pflush_worksets *worksets,
                               struct flush_load **per_worker_loads, struct flush_load *per_socket_loads) {
    worksets->load_balance_ws.per_socket_loads = per_socket_loads;
    launch_workers(worksets, load_balance_work, &worksets->load_balance_ws);
    *per_worker_loads = worksets->load_balance_ws.per_worker_loads;
}

static void flush_stage(struct pflush_worksets *worksets, struct flush_load *per_worker_loads) {
    worksets->flush_ws.per_worker_loads = per_worker_loads;
    launch_workers(worksets, flush_work, &worksets->flush_ws);
}

static void cleanup_stage(struct pflush_worksets *worksets) {
    cleanup_logs(worksets->fetch_ws.new_region_starts);

    for (int i = 0; i < NUM_PFLUSH_WORKER; i++) {
        shim_recycle(worksets->flush_ws.shim_recycle_chains[i]);
    }

    pnode_recycle();

    for (int i = 0; i < NUM_PFLUSH_WORKER; i++) {
        flush_load_destroy(&worksets->flush_ws.per_worker_loads[i]);
    }

    for (int i = 0; i < NUM_PFLUSH_WORKER; i++) {
        for (int j = 0; j < NUM_SOCKET; j++) {
            free(worksets->clustering_ws.pbatch_ops[i][j]);
        }
    }
}

/*
 * oplog_flush: perform a full log flush
 */
void oplog_flush() {
    struct log_layer *l_layer = LOG(bonsai);

    struct flush_load *per_socket_loads, *per_worker_loads;
    struct pflush_worksets ws;
    logs_t *per_worker_logs;
    unsigned since;

	bonsai_print("thread[%d]: start oplog checkpoint [%d]\n", __this->t_id, l_layer->nflush);

	atomic_set(&l_layer->checkpoint, 1);

    new_flip();
    bonsai_print("Enter flip %d\n", l_layer->lst.flip);

    begin_invalidate_unref_entries(&since);

    /* Make sure that all logs in previous flip are written back to NVM. */
    force_wb();

    /* init work */
    bonsai_print("oplog_flush: init stage\n");
    init_stage(&ws);

    /* fetch all current-flip logs in NVM to DRAM buffer */
    bonsai_print("oplog_flush: fetch stage\n");
    fetch_stage(&ws, &per_worker_logs);

    /* sort, merge, and cluster based on pnode */
    bonsai_print("oplog_flush: cluster stage\n");
    cluster_stage(&ws, &per_socket_loads, per_worker_logs);

    /* inter and intra socket load balance */
    bonsai_print("oplog_flush: balance stage\n");
    load_balance_stage(&ws, &per_worker_loads, per_socket_loads);

    end_invalidate_unref_entries(&since);

    /* flush and sync */
    bonsai_print("oplog_flush: flush stage\n");
    flush_stage(&ws, per_worker_loads);

    /* cleanup work */
    cleanup_stage(&ws);

	l_layer->nflush++;
	atomic_set(&l_layer->checkpoint, 0);

	bonsai_print("thread[%d]: finish log checkpoint [%d]\n", __this->t_id, l_layer->nflush);
}

static int register_wb_signal() {
	struct sigaction sa;
	int ret = 0;

	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sa.sa_handler = handle_wb;
	if (sigaction(WB_SIGNO, &sa, NULL) == -1) {
		perror("sigaction\n");
        goto out;
	}

out:
	return ret;
}

int log_layer_init(struct log_layer* layer) {
	int i, ret = 0, node, dimm_idx, dimm, cpu_idx, cpu;
    struct cpu_log_region_desc *desc;
    pthread_mutex_t *dimm_lock;

	layer->nflush = 0;
	atomic_set(&layer->exit, 0);
	atomic_set(&layer->force_flush, 0);
	atomic_set(&layer->checkpoint, 0);
	atomic_set(&layer->epoch_passed, 0);
    for (i = 0; i < NUM_CPU; i++) {
        atomic_set(&layer->nlogs[i].cnt, 0);
    }

	layer->lst.flip = 0;

    ret = log_region_init(layer);
	if (ret)
		goto out;

	layer->desc = malloc(sizeof(struct log_region_desc));

    for (node = 0; node < NUM_SOCKET; node++) {
        cpu_idx = 0;

        for (dimm_idx = 0; dimm_idx < NUM_DIMM_PER_SOCKET; dimm_idx++) {
            dimm = node_idx_to_dimm(node, dimm_idx);

            dimm_lock = malloc(sizeof(pthread_mutex_t));

            for (i = 0; i < NUM_CPU_PER_LOG_DIMM; i++, cpu_idx++) {
                cpu = node_idx_to_cpu(node, cpu_idx);

                desc = &layer->desc->descs[cpu];
                desc->region = &layer->dimm_regions[dimm]->regions[i];
                desc->size = 0;
                desc->lcb = malloc(LCB_MAX_SIZE * sizeof(*desc->lcb));
                desc->wb_state = WBS_ENABLE;
                desc->dimm_lock = dimm_lock;
                seqcount_init(&desc->seq);
            }
        }
    }

    register_wb_signal();

	bonsai_print("log_layer_init\n");

out:
	return ret;
}

void log_layer_deinit(struct log_layer* layer) {
	//struct cpu_log_region_desc *desc;
	int node, cpu;

	for (node = 0; node < NUM_SOCKET; node++) {
		for (cpu = 0; cpu < NUM_CPU_PER_LOG_DIMM; cpu++) {
			//desc = &layer->desc->descs[cpu];
		}
	}
	
	log_region_deinit(layer);

	bonsai_print("log_layer_deinit\n");
}
