/*
 * BonsaiKV: A Fast, Scalable, Persistent Key-Value Store for DRAM-NVM Systems
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 * 				 Junru Shen, gnu_emacs@hhu.edu.cn
 */
#define _GNU_SOURCE
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
#include "cpu.h"
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

#define OPLOG_TYPE(t)   ((t) & 2)
#define OPLOG_FLIP(t)   ((t) & 1)

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
    /* The output of the fetch work. Collected logs for each worker. */
    logs_t per_worker_logs[NUM_PFLUSH_WORKER];

    uint32_t new_region_starts[NUM_CPU];
};

struct clustering_workset {
    /* The input of the cluster work. Collected logs for each worker. */
    logs_t *per_worker_logs;

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
};

struct pflush_worksets {
    struct desc_workset         desc_ws;
    struct fetch_workset        fetch_ws;
    struct clustering_workset   clustering_ws;
    struct load_balance_workset load_balance_ws;
    struct flush_workset        flush_ws;
};

static inline size_t max_load_per_socket(size_t avg_load) {
    return avg_load + 1000000;
}

static inline int oplog_overflow_size(struct cpu_log_region_meta *meta, uint32_t off) {
    uint32_t d1 = (meta->end - meta->start + OPLOG_NUM_PER_CPU) % OPLOG_NUM_PER_CPU;
    uint32_t d2 = (off - meta->start + OPLOG_NUM_PER_CPU) % OPLOG_NUM_PER_CPU;
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

static void write_back(int cpu, int dimm_unlock) {
	struct log_layer* layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;
    uint32_t end = region->meta.end;
    struct oplog *lcb;
    size_t len, c;

    lcb = local_desc->lcb;
    len = local_desc->size;

    while ((c = min(OPLOG_NUM_PER_CPU - end, len))) {
        memcpy_nt(&region->logs[end], lcb, c * sizeof(struct oplog), 0);
        end  = (end + c) % OPLOG_NUM_PER_CPU;
        len -= c;
        lcb += c;
    }
    memory_sfence();

    if (dimm_unlock) {
        pthread_mutex_unlock(local_desc->dimm_lock);
    }

    lcb = malloc(LCB_MAX_SIZE * sizeof(*lcb));

    local_desc->size = 0;
    call_rcu(RCU(bonsai), free, local_desc->lcb);

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
            write_back(cpu, 0);
            local_desc->wb_done = 1;
            break;

        case WBS_DELAY:
            local_desc->wb_state = WBS_REQUEST;
            break;

        default:
            assert(0);
    }
}

logid_t oplog_insert(pkey_t key, pval_t val, optype_t op, int cpu) {
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
    id.off = (local_desc->size + end) % OPLOG_NUM_PER_CPU;

    log = &local_desc->lcb[local_desc->size++];
	log->o_type = cpu_to_le8(op);
    log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_kv.k = key;
	log->o_kv.v = val;

    if (unlikely(local_desc->size >= LCB_FULL_NR 
			&& local_desc->size % 4 == 0)) {
        if (pthread_mutex_trylock(local_desc->dimm_lock)) {
            write_back(cpu, 1);
        } else if (unlikely(local_desc->size >= LCB_MAX_NR)) {
            write_back(cpu, 0);
        }
    }

    if (cmpxchg_local(&local_desc->wb_state, WBS_DELAY, WBS_ENABLE) == WBS_REQUEST) {
        write_back(cpu, 0);
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
	xadd(&LOG(bonsai)->lst.flip, ACCESS_ONCE(LOG(bonsai)->lst.flip) ? -1 : 1);
	
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
    return (snap->region_end - snap->region_start + OPLOG_NUM_PER_CPU) % OPLOG_NUM_PER_CPU;
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

static size_t fetch_cpu_logs(uint32_t *new_region_starts, struct oplog *logs, struct cpu_log_snapshot *snap) {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[snap->cpu];
    struct cpu_log_region *region = local_desc->region;
    int target_flip = !layer->lst.flip;
    struct oplog *plog, *log;
    uint32_t cur, end;
	size_t size = sizeof(*log);

    cur = snap->region_start;
    end = snap->region_end;

	printf("flip: %d\n", target_flip);

    for (log = logs; cur != end; cur = (cur + 1) % OPLOG_NUM_PER_CPU, log++) {
        plog = &region->logs[cur];
		printf("<%lu, %lu>\n", plog->o_kv.k.key, plog->o_kv.v);
		if (unlikely(OPLOG_FLIP(plog->o_type) != target_flip)) {
            break;
        }
        memcpy(log, plog, size);
    }

    new_region_starts[snap->cpu] = end;

    return log - logs;
}

static logs_t fetch_logs(uint32_t *new_region_starts, int nr_cpu, int *cpus) {
    struct cpu_log_snapshot snapshots[NUM_CPU];
    logs_t fetched = { .cnt = 0 };
    struct oplog *log;
    int i;

    for (i = 0; i < nr_cpu; i++) {
        snapshot_cpu_log(&snapshots[i], cpus[i]);
        fetched.cnt += (int) log_nr(&snapshots[i]);
		printf("fetch cpu[%d] nlog %d\n", snapshots[i].cpu, (int) log_nr(&snapshots[i]));
	}

    log = fetched.logs = malloc(sizeof(*log) * fetched.cnt);

    for (i = 0; i < nr_cpu; i++) {
        log += fetch_cpu_logs(new_region_starts, log, &snapshots[i]);
    }

    return fetched;
}

/*
 * Fetch worker
 * O: per_worker_logs
 */
static int fetch_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct fetch_workset *ws = desc->workset;
    int tot = NUM_CPU / NUM_PFLUSH_WORKER;
    int wid = desc->wid, wnr, numa_node;
    int i, nr_cpu, cpus[tot];

    numa_node = worker_numa_node(wid);
    wnr = worker_nr(wid);

    for (i = tot * wnr, nr_cpu = 0; tot--; i++, nr_cpu++) {
        cpus[nr_cpu] = node_to_cpu(numa_node, i);	
    }

    ws->per_worker_logs[wid] = fetch_logs(ws->new_region_starts, nr_cpu, cpus);

    return 0;
}

/* Inline the sort function to reduce overhead caused by frequently making indirect call to cmp. */
#define num_cmp(x, y)           ((x) == (y) ? 0 : ((x) < (y) ? -1 : 1))
#define sort_cmp(a, b, arg)     (pkey_compare(((struct oplog *) (a))->o_kv.k, ((struct oplog *) (b))->o_kv.k) \
                                 ? : num_cmp(((struct oplog *) (a))->o_stamp, ((struct oplog *) (b))->o_stamp))
#include "sort.h"

static void collaboratively_sort_logs(logs_t *logs, pthread_barrier_t *barrier, int wid) {
	int phase, n = NUM_PFLUSH_WORKER, i, left, oid, cnt, ocnt, delta;
    struct oplog *ent, *oent, *nent, *nptr, *data, *odata, **get;

    data = logs[wid].logs;
    cnt = logs[wid].cnt;

	printf("sort %d logs, %016lx\n", cnt, data);

    do_qsort(data, cnt, sizeof(struct oplog), NULL);

	for (phase = 0; phase < n; phase++) {
        /* Wait for the last phase to be done. */
	    pthread_barrier_wait(barrier);

        bonsai_print("[pflush worker %d] collaboratively_sort_logs: phase %d\n", wid, phase);

        /* Am I the left half in this phase? */
        left = wid % 2 == phase % 2;
        delta = left ? 1 : -1;
        oid = wid + delta;
        if (oid < 0 || oid >= NUM_PFLUSH_WORKER) {
            pthread_barrier_wait(barrier);
            continue;
        }

        ocnt = logs[oid].cnt;
        odata = logs[oid].logs;

        ent = left ? data : (data + cnt - 1);
        oent = left ? odata : (odata + ocnt - 1);

        nent = malloc(sizeof(struct oplog) * cnt);
        nptr = left ? nent : (nent + cnt - 1);

        for (i = 0; i < cnt; i++) {
            if (unlikely(ent - data == cnt || ent < data)) {
                get = &oent;
            } else if (unlikely(oent - odata == ocnt || oent < odata)) {
                get = &ent;
            } else {
                if (!pkey_compare(ent->o_kv.k, oent->o_kv.k)) {
                    get = &ent;
                } else {
                    get = (!left ^ (pkey_compare(ent->o_kv.k, oent->o_kv.k) < 0)) ? &ent : &oent;
                }
            }
            *nptr = **get;
            *get += delta;
            nptr += delta;
        }

	    pthread_barrier_wait(barrier);

        free(data);
        logs[wid].logs = data = nent;
	}
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

static void merge_and_cluster_logs(struct flush_load *per_socket_loads, logs_t *logs,
                                   pthread_barrier_t *barrier, int wid) {
    int i, next_wid, total = logs[wid].cnt, numa_node = 0;
    struct list_head pbatch_lists[NUM_SOCKET];
    pbatch_cursor_t cursors[NUM_SOCKET];
    struct oplog *log = logs[wid].logs;
    pbatch_op_t *ops;

    if (unlikely(!total)) {
        return;
    }

    for (i = 0; i < NUM_SOCKET; i++) {
        ops = malloc(total * sizeof(*ops));
        INIT_LIST_HEAD(&pbatch_lists[i]);
        pbatch_list_create(&pbatch_lists[i], ops, total, 1);
        pbatch_cursor_init(&cursors[i], &pbatch_lists[i]);
        INIT_LIST_HEAD(&per_socket_loads[i].cluster);
        per_socket_loads[i].load = 0;
    }

    for (; --total; log++) {
        if (pkey_compare(log[0].o_kv.k, log[1].o_kv.k)) {
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

    pthread_barrier_wait(barrier);

    free(logs[wid].logs);
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

        if (likely(!list_empty(target))) {
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
    collaboratively_sort_logs(ws->per_worker_logs, &ws->barrier, wid);
    
    /* Merge and cluster the per_worker_logs. (per_worker_logs -> per-worker per-socket loads) */
    merge_and_cluster_logs(ws->per_worker_per_socket_loads[wid], ws->per_worker_logs, &ws->barrier, wid);

    /* Do global inter-worker cluster. (per-worker per-socket loads -> per-socket loads) */
    inter_worker_cluster(ws->per_socket_loads, ws->per_worker_per_socket_loads, &ws->barrier, wid);

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
            for (victim = 0; victim < NUM_SOCKET && per_socket_loads[node].load >= avg_load; victim++);
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
    if (wid == 0) {
        /* We only want one global worker to do this now. */
        do_inter_socket_load_balance(per_socket_loads);
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
        per_worker_loads[worker_id(numa_node, i)] = per_socket_per_worker_loads[i];
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

    intra_socket_load_balance(ws->per_worker_loads, ws->per_socket_loads, &ws->barrier, wid);

    return 0;
}

static int flush_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct flush_workset *ws = desc->workset;
    struct flush_load *load = &ws->per_worker_loads[desc->wid];
    struct cluster *c, *tmp;

    list_for_each_entry_safe(c, tmp, &load->cluster, list) {
        pnode_run_batch(NULL, c->pnode, &c->pbatch_list);

        pbatch_list_destroy(&c->pbatch_list);
        list_del(&c->list);
        free(c);
    }
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

    pnode_recycle();
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
	int i, ret = 0, node, dimm_idx, dimm, cpu;
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

	layer->desc = (struct log_region_desc*)malloc(sizeof(struct log_region_desc));

    for (node = 0, cpu = 0; node < NUM_SOCKET; node++) {
        for (dimm_idx = 0; dimm_idx < NUM_DIMM_PER_SOCKET; dimm_idx ++) {
            dimm = node_to_dimm(node, dimm_idx);

            dimm_lock = malloc(sizeof(pthread_mutex_t));

            for (i = 0; i < NUM_CPU_PER_LOG_DIMM; i++, cpu++) {
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
	struct cpu_log_region_desc *desc;
	int node, cpu;

	for (node = 0; node < NUM_SOCKET; node++) {
		for (cpu = 0; cpu < NUM_CPU_PER_LOG_DIMM; cpu++) {
			desc = &layer->desc->descs[cpu];
			free(desc->lcb);
		}
	}
	
	log_region_deinit(layer);

	bonsai_print("log_layer_deinit\n");
}
