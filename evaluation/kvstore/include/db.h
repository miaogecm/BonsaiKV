#ifndef DB_H
#define DB_H

#define db_lookup(kv_type, key, num_v, col_arr, size_arr, v_arr) \
        ({int __col_i, __ret; \
        for (__col_i = 0; __col_i < num_v; __col_i++) { \
            key.col = col_arr[__col_i]; \
            __ret = kvstore->kv_get(tcontext, &key, sizeof(key), v_arr[__col_i], &size_arr[__col_i]); \
        }; \
        __ret;})

#define db_insert(kv_type, key, val, num_v, size_arr) \
        ({int __col_i; void* __ptr = &val; \
        for (__col_i = 1; __col_i <= num_v; __col_i++) { \
            key.col = __col_i; \
            kvstore->kv_put(tcontext, &key, sizeof(key), __ptr, size_arr[__col_i]); \
            __ptr += size_arr[__col_i]; \
        }})

#define db_update(kv_type, key, num_v, col_arr, size_arr, v_arr) \
        ({int __col_i; \
        for (__col_i = 0; __col_i < num_v; __col_i++) { \
            key.col = col_arr[__col_i]; \
            kvstore->kv_put(tcontext, &key, sizeof(key), v_arr[__col_i], size_arr[__col_i]); \
        }})

#endif