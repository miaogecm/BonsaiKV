## Generic KVStore Benchmarking Framework

### Built-in Runners

Currently, 3 runners are supported:

+ TATP Runner
+ TPCC Runner
+ YCSB Runner

### Generic KVStore Interface

Each KVStore should be wrapped to a dynamically linked library. It should implement these interfaces (maybe an adapter layer is needed):

```C
const char *(*kv_engine)();
void *(*kv_create_context)(void *config);
void (*kv_destroy_context)(void *context);
void (*kv_start_test)(void *context);
void (*kv_stop_test)(void *context);
void *(*kv_thread_create_context)(void *context, int id);
void (*kv_thread_destroy_context)(void *tcontext);
void (*kv_thread_start_test)(void *tcontext);
void (*kv_thread_stop_test)(void *tcontext);
void (*kv_txn_begin)(void *tcontext);
void (*kv_txn_rollback)(void *tcontext);
void (*kv_txn_commit)(void *tcontext);
int (*kv_put)(void *tcontext, void *key, size_t key_len, void *val, size_t val_len);
int (*kv_del)(void *tcontext, void *key, size_t key_len);
int (*kv_get)(void *tcontext, void *key, size_t key_len, void *val, size_t *val_len);
void (*kv_scan)(void *tcontext, void *key, size_t key_len, int range, void *values);
```

### Loader

KVStore is loaded using `load_kvstore` function.

```C
void load_kvstore(struct kvstore *kvstore, const char *libpath);
```

### Runner

A `run` contains many `stages`. For example, in `YCSB`, we have `load` and `op` stage; In `TPCC`, we have `load` and `txn` stage. KVStore is run using `run_kvstore` function.

```C
void run_kvstore(struct kvstore *kv, void *conf, int nr_stage,
                 const char *(*stage_func[])(struct kvstore *kv, void *tcontext, int));
```

To add a new `runner`, you only need to call `load_kvstore` and `run_kvstore` properly. You should write some stage functions, which defines what to do in each test stage, and pass them to `run_kvstore`.

