#ifndef COMPRESSOR_COMPRESSOR_H
#define COMPRESSOR_COMPRESSOR_H

#include <stdint.h>

#define STR_KEY_LEN 24

enum op_type {
    OP_INSERT = 0,
    OP_UPDATE,
    OP_READ,
    OP_SCAN
};

static struct op_string_key {
    char         key[STR_KEY_LEN];
    enum op_type type;
    int          range;
} op_string_key;

static struct op_integer_key {
    uint64_t     key;
    enum op_type type;
    int          range;
} op_integer_key;

#endif //COMPRESSOR_COMPRESSOR_H
