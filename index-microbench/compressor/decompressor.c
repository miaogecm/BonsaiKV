#include <stdio.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <assert.h>

#include "compressor.h"
#include "decompressor.h"

void ycsb_decompressor_init(ycsb_decompressor_t *dec, const char *path, int is_str_key) {
    int fd = open(path, O_RDONLY);
    struct stat info;
    stat(path, &info);
    dec->size = info.st_size;
    dec->start = mmap(NULL, info.st_size, PROT_READ, MAP_FILE, fd, 0);
    dec->is_str_key = is_str_key;
}

void ycsb_decompressor_deinit(ycsb_decompressor_t *dec) {
    munmap(dec->start, dec->size);
}

int ycsb_decompressor_get_nr(ycsb_decompressor_t *dec) {
    size_t size_per_op = dec->is_str_key ? sizeof(struct op_string_key) : sizeof(struct op_integer_key);
    assert(dec->size % size_per_op == 0);
    return (int) (dec->size / size_per_op);
}

enum op_type ycsb_decompressor_get(ycsb_decompressor_t *dec, void *key, int *range, int i) {
    if (dec->is_str_key) {
        struct op_string_key *k = dec->start;
        *(char **) key = k[i].key;
        *range = k[i].range;
        return k->type;
    } else {
        struct op_integer_key *k = dec->start;
        *(uint64_t *) key = k[i].key;
        *range = k[i].range;
        return k->type;
    }
}
