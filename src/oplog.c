/*
 * Bonsai: Transparent, Scalable, NUMA-aware Persistent Data Store
 *
 * Hohai University
 *
 * Author: Miao Cai, mcai@hhu.edu.cn
 */
#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "bonsai.h"
#include "oplog.h"
#include "ordo.h"
#include "arch.h"
#include "cpu.h"
#include "region.h"
#include "common.h"
#include "thread.h"
#include "pnode.h"
#include "shim.h"
#include "hash.h"
#include "rcu.h"

#define LCB_FULL_SIZE   3072
#define LCB_MAX_SIZE    (LCB_FULL_SIZE * 2)

#define LCB_FULL_NR     (LCB_FULL_SIZE / sizeof(struct oplog))
#define LCB_MAX_NR      (LCB_MAX_SIZE / sizeof(struct oplog))

#define OPLOG_TYPE(t)   ((t) & 2)
#define OPLOG_TURN(t)   ((t) & 1)

#define CHECK_NLOG_INTERVAL     20

union logid_u {
    logid_t id;
    struct {
        uint32_t cpu : 7;
        uint32_t off : 25;
    };
};

struct pflush_work_desc {
    int wid, cpu;
    void *workset;
};

typedef struct {
    struct oplog *logs;
    int cnt;
} logs_t;

struct worker_load {
    size_t load;
    struct list_head cluster;
};

struct cluster_workset {
    logs_t logs[NUM_PFLUSH_WORKER];

    struct worker_load loads[NUM_PFLUSH_WORKER][NUM_PFLUSH_WORKER];

    pthread_barrier_t barrier;
};

struct cluster {
    pnoid_t pnode;
    struct list_head pbatch_list;
    struct list_head list;
};

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
    int overflow;
    desc = &LOG(bonsai)->desc->descs[id.cpu];
    meta = desc->region->meta;
    overflow = oplog_overflow_size(&meta, id.off);
    if (unlikely(overflow >= 0)) {
        return &desc->lcb[overflow];
    } else {
        return &desc->region->logs[id.off];
    }
}

logid_t oplog_insert(pkey_t key, pval_t val, optype_t op, int numa_node, int cpu) {
	struct log_layer* layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;
    uint32_t end = region->meta.end;
    static __thread int last = 0;
	struct oplog* log;
    union logid_u id;
    size_t len, c;
    int wb = 0;
    void *lcb;

    assert(local_desc->size < LCB_MAX_NR);

    id.cpu = cpu;
    id.off = (local_desc->size + end) % OPLOG_NUM_PER_CPU;

    log = &local_desc->lcb[local_desc->size++];
	log->o_type = cpu_to_le8(op);
    log->o_stamp = cpu_to_le64(ordo_new_clock(0));
	log->o_kv.k = key;
	log->o_kv.v = val;

    if (unlikely(local_desc->size >= LCB_FULL_NR)) {
        if (pthread_mutex_trylock(local_desc->dimm_lock)) {
            wb = 1;
        } else if (unlikely(local_desc->size >= LCB_MAX_NR)) {
            pthread_mutex_lock(local_desc->dimm_lock);
            wb = 1;
        }

        if (wb) {
            lcb = local_desc->lcb;
            len = local_desc->size;

            while ((c = max(OPLOG_NUM_PER_CPU - end, len))) {
                memcpy_nt(&region->logs[end], lcb, c * sizeof(struct oplog), 0);
                end  = (end + c) % OPLOG_NUM_PER_CPU;
                len -= c;
                lcb += c;
            }
            memory_sfence();

            pthread_mutex_unlock(local_desc->dimm_lock);

            region->meta.end = end;
            bonsai_flush(&region->meta.end, sizeof(__le32), 1);

            local_desc->size = 0;
            call_rcu(RCU(bonsai), free, local_desc->lcb);
            local_desc->lcb = malloc(LCB_MAX_SIZE);
        }
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

static void new_turn() {
    rcu_t *rcu = RCU(bonsai);
    xadd(&LOG(bonsai)->lst.turn, 1);
    /* Wait for all the user threads to observe the latest turn. */
    rcu_synchronize(rcu, rcu_now(rcu));
}

static logs_t fetch_cpu_logs(int cpu) {
	struct log_layer *layer = LOG(bonsai);
    struct cpu_log_region_desc *local_desc = &layer->desc->descs[cpu];
    struct cpu_log_region *region = local_desc->region;
    int target_turn = ~layer->lst.turn;
    struct oplog *plog, *logs, *log;
    uint32_t cur, end;
    size_t total;

    /* TODO: Fetch those in DRAM LCB!!! */

    cur = region->meta.start;
    end = region->meta.end;

    total = (end - cur + OPLOG_NUM_PER_CPU) % OPLOG_NUM_PER_CPU;
    logs = malloc(total);

    for (log = logs; cur != end; cur = (cur + 1) % OPLOG_NUM_PER_CPU, log++) {
        plog = &region->logs[cur];
        if (unlikely(OPLOG_TURN(plog->o_type) != target_turn)) {
            break;
        }

        memcpy(log, plog, sizeof(*log));
    }

    return (logs_t) { logs, (int) (log - logs) };
}

/* Inline the sort function to reduce overhead caused by frequently making indirect call to cmp. */
#define num_cmp(x, y)           ((x) == (y) ? 0 : ((x) < (y) ? -1 : 1))
#define sort_cmp(a, b, arg)     (pkey_compare(((struct oplog *) (a))->o_kv.k, ((struct oplog *) (b))->o_kv.k) \
                                 ? : num_cmp(((struct oplog *) (a))->o_ts, ((struct oplog *) (b))->o_ts))
#include "sort.h"

static void collaboratively_sort_logs(pthread_barrier_t *barrier, logs_t *logs, int wid) {
	int phase, n = NUM_PFLUSH_WORKER, i, left, oid, cnt, ocnt, delta;
    struct oplog *ent, *oent, *nent, *nptr, *data, *odata, **get;

    data = logs[wid].logs;
    cnt = logs[wid].cnt;

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

static inline int pnode_assignee(pnoid_t pnode) {
    /* TODO: Better hash function */
    /* TODO: NUMA-Aware assignee */
    return pnode % NUM_PFLUSH_WORKER;
}

static void pbatch_op_add(struct worker_load *loads, pbatch_cursor_t *cursors, int *assignee, struct oplog *log) {
    struct list_head *cluster_list;
    struct cluster *cluster;
    pbatch_cursor_t *cursor;
    pbatch_op_t *op;
    pnoid_t pnode;
    int split = 0;

    /* Are we still in the right pnode? */
    cluster_list = &loads[*assignee].cluster;
    cluster = list_last_entry(cluster_list, struct cluster, list);
    if (list_empty(cluster_list) || !is_in_pnode(cluster->pnode, log->o_kv.k)) {
        pnode = shim_pnode_of(log->o_kv.k);
        *assignee = pnode_assignee(pnode);
        split = 1;
    }

    cursor = &cursors[*assignee];

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
        list_add_tail(&cluster->list, &loads[*assignee].cluster);
        pbatch_list_split(&cluster->pbatch_list, cursor);
    }

    /* Forward the cursor. */
    pbatch_cursor_inc(cursor);
}

static void merge_and_cluster_logs(struct worker_load *loads, logs_t *logs, int wid) {
    int i, next_wid, total = logs[wid].cnt, assignee = 0;
    struct list_head pbatch_lists[NUM_PFLUSH_WORKER];
    pbatch_cursor_t cursors[NUM_PFLUSH_WORKER];
    struct oplog *log = logs[wid].logs;
    pbatch_op_t *ops;

    if (unlikely(!total)) {
        return;
    }

    for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
        ops = malloc(total * sizeof(*ops));
        INIT_LIST_HEAD(&pbatch_lists[i]);
        pbatch_list_create(&pbatch_lists[i], ops, total, 1);
        pbatch_cursor_init(&cursors[i], &pbatch_lists[i]);
    }

    for (; --total; log++) {
        if (pkey_compare(log[0].o_kv.k, log[1].o_kv.k)) {
            pbatch_op_add(loads, cursors, &assignee, log);
        }
    }

    for (next_wid = wid + 1; next_wid < NUM_PFLUSH_WORKER && !logs[next_wid].cnt; next_wid++);
    if (next_wid >= NUM_PFLUSH_WORKER || pkey_compare(logs[next_wid].logs[0].o_kv.k, log->o_kv.k)) {
        pbatch_op_add(loads, cursors, &assignee, log);
    }

    /* Remove the trailing empty space. */
    for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
        if (!pbatch_cursor_is_end(&cursors[i])) {
            pbatch_list_split(&pbatch_lists[i], &cursors[i]);
            pbatch_list_destroy(&pbatch_lists[i]);
        }
    }
}

static void clustering_work(void *arg) {
    struct pflush_work_desc *desc = arg;
    struct cluster_workset *ws = desc->workset;
    struct worker_load *loads;
    int wid = desc->wid, i;

    /* Fetch all the logs. */
    /* TODO: It's wrong. It should consider all the user threads. */
    ws->logs[wid] = fetch_cpu_logs(desc->cpu);

    /* Sort them collaboratively. */
    collaboratively_sort_logs(&ws->barrier, ws->logs, wid);
    
    /* Merge and cluster the logs. */
    loads = ws->loads[wid];
    for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
        INIT_LIST_HEAD(&loads[i].cluster);
    }
    merge_and_cluster_logs(loads, ws->logs, wid);

    /* Free logs */
    pthread_barrier_wait(&ws->barrier);
    free(ws->logs[wid].logs);
}

static void global_clustering(struct worker_load *loads, struct worker_load *loads_per_worker[NUM_PFLUSH_WORKER]) {
    struct worker_load *load, *load_per_worker;
    struct list_head *addon, *target;
    struct cluster *prev, *curr;
    int assignee, i;

    for (assignee = 0; assignee < NUM_PFLUSH_WORKER; assignee++) {
        load = &loads[assignee];

        load->load = 0;
        target = &load->cluster;
        INIT_LIST_HEAD(target);

        for (i = 0; i < NUM_PFLUSH_WORKER; i++) {
            load_per_worker = &loads_per_worker[i][assignee];

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
}

static void load_balance(struct worker_load *loads) {

}

/*
 * oplog_flush: perform a full log flush
 */
void oplog_flush() {
	struct log_layer *l_layer = LOG(bonsai);
    struct dimm_log_region *region;
	volatile struct oplog_blk* first_blks[NUM_CPU_PER_SOCKET];
	volatile struct oplog_blk* curr_blks[NUM_CPU_PER_SOCKET];
	volatile struct oplog_blk* curr_blk;
	struct oplog_work* owork;
	struct oplog_work* oworks[NUM_SOCKET][NUM_PFLUSH_WORKER_PER_NODE];
	struct work_struct* work;
    int i, j, cpu, cnt, total, node, cpu_idx;
	int num_work, num_block, num_block_per_worker, num_busy_worker;
    int empty = 1;
    struct oplog_work empty_owork[NUM_SOCKET][NUM_PFLUSH_WORKER_PER_NODE] = { };

	bonsai_print("thread[%d]: start oplog checkpoint [%d]\n", __this->t_id, l_layer->nflush);

	atomic_set(&l_layer->checkpoint, 1);

    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            empty_owork[node][i].layer = l_layer;
            empty_owork[node][i].node = node;
            empty_owork[node][i].id = i;
        }
    }

    /************************************************
     *   Per-node Fetch and Distribute Log Lists    *
     ************************************************/

    for (node = 0; node < NUM_SOCKET; node++) {
        total = 0;
        cnt = 0;

        /* 1. fetch and merge all log lists */
        for (cpu_idx = 0; cpu_idx < NUM_CPU_PER_SOCKET; cpu_idx++) {
            cpu = node_to_cpu(node, cpu_idx);
            region = &l_layer->region[cpu];
            curr_blk = ACCESS_ONCE(region->curr_blk);
            /* TODO: Non-full log blocks can not be flushed! */
            if (curr_blk && region->first_blk != curr_blk) {
                first_blks[total] = region->first_blk;
                curr_blks[total] = curr_blk;
                region->first_blk = curr_blk;
                total++;
            }
        }

        if (unlikely(!total)) {
            for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
                oworks[node][i] = &empty_owork[node][i];
            }
            continue;
        } else {
            empty = 0;
        }

        if (unlikely(atomic_read(&l_layer->exit))) {
            goto out;
        }

        /* 2. merge all lps */

        /* count number of log blocks */
        num_block = 0;
        for (i = 0; i < total; i++) {
            for (curr_blk = first_blks[i];
                 curr_blk != curr_blks[i];
                 curr_blk = (struct oplog_blk *) LOG_REGION_OFF_TO_ADDR((&l_layer->region[curr_blk->cpu]),
                                                                        curr_blk->next)) {
                num_block++;
            }
        }
        num_block_per_worker = num_block / NUM_PFLUSH_WORKER_PER_NODE;
        num_busy_worker = num_block % NUM_PFLUSH_WORKER_PER_NODE;
        bonsai_print("%d log blocks to proceed, num_block_per_worker=%d, num_busy_worker=%d\n",
                     num_block, num_block_per_worker, num_busy_worker);

        /* distribute work to workers */
        j = 0;
        curr_blk = first_blks[j];

        for (i = 0; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            cnt = num_work = i < num_busy_worker ? (num_block_per_worker + 1) : num_block_per_worker;

            if (unlikely(!cnt)) {
                owork = &empty_owork[node][i];
            } else {
                owork = malloc(sizeof(struct oplog_work));
                owork->count = 0;
                owork->layer = l_layer;
                INIT_LIST_HEAD(&owork->free_page_list);
            }

            while (cnt) {
                owork->first_blks[owork->count] = curr_blk;
                for (; curr_blk != curr_blks[j] && cnt;
                       curr_blk = (struct oplog_blk *) LOG_REGION_OFF_TO_ADDR((&l_layer->region[curr_blk->cpu]),
                                                                              curr_blk->next)) {
                    cnt--;
                }
                owork->last_blks[owork->count++] = curr_blk;

                if (curr_blk == curr_blks[j]) {
                    curr_blk = first_blks[++j];
                }
            }

            oworks[node][i] = owork;

            bonsai_print("pflush thread <%d, %d> assigned %d blocks in %d regions.\n",
                         node, i, num_work, owork->count);
        }
    }

    if (unlikely(empty)) {
        goto out;
    }

    /************************************************
     *      Per-thread collect and sort lps        *
     ************************************************/

    for (node = 0; node < NUM_SOCKET; node++) {
        for (cnt = 0; cnt < NUM_PFLUSH_WORKER_PER_NODE; cnt++) {
            oworks[node][cnt]->node = node;
            oworks[node][cnt]->id = cnt;
            work = malloc(sizeof(struct work_struct));
            INIT_LIST_HEAD(&work->list);
            work->exec = worker_oplog_collect_and_sort;
            work->exec_arg = (void *) (oworks[node][cnt]);
            workqueue_add(&bonsai->pflush_workers[node][cnt]->t_wq, work);
        }
    }

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 3. flush all lps */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (cnt = 0; cnt < NUM_PFLUSH_WORKER_PER_NODE; cnt++) {
            work = malloc(sizeof(struct work_struct));
            INIT_LIST_HEAD(&work->list);
            work->exec = worker_oplog_flush;
            work->exec_arg = (void *) (oworks[node][cnt]);
            workqueue_add(&bonsai->pflush_workers[node][cnt]->t_wq, work);
        }
    }

	wakeup_workers();

	park_master();

	if (unlikely(atomic_read(&l_layer->exit))) 
		goto out;

	/* 4. free all pages */
    for (node = 0; node < NUM_SOCKET; node++) {
        for (i = 1; i < NUM_PFLUSH_WORKER_PER_NODE; i++) {
            if (oworks[node][i] < &empty_owork[0][0] ||
                oworks[node][i] > &empty_owork[NUM_SOCKET - 1][NUM_PFLUSH_WORKER_PER_NODE - 1]) {
                free_pages(l_layer, &oworks[node][i]->free_page_list);
                free(oworks[node][i]);
            }
        }
    }

out:
	/* 5. finish */
	l_layer->nflush ++;
	atomic_set(&l_layer->checkpoint, 0);

	bonsai_print("thread[%d]: finish log checkpoint [%d]\n", __this->t_id, l_layer->nflush);
}
