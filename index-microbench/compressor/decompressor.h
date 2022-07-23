#ifndef COMPRESSOR_DECOMPRESSOR_H
#define COMPRESSOR_DECOMPRESSOR_H

typedef struct {
    void *start;
    size_t size;
    int is_str_key;
} ycsb_decompressor_t;

void ycsb_decompressor_init(ycsb_decompressor_t *dec, const char *path, int is_str_key);
void ycsb_decompressor_deinit(ycsb_decompressor_t *dec);
int ycsb_decompressor_get_nr(ycsb_decompressor_t *dec);
enum op_type ycsb_decompressor_get(ycsb_decompressor_t *dec, void *key, int *range, int i);

#endif //COMPRESSOR_DECOMPRESSOR_H
