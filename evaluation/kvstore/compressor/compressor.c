#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "compressor.h"

static int is_str_key, finished = 0;
static FILE *in, *out;
static char buf[4096];

static inline void append(enum op_type type, char *key, int range) {
    static uint16_t tick = -1;

    if (is_str_key) {
        memset(op_string_key.key, 0, STR_KEY_LEN);
        memcpy(op_string_key.key, key, strlen(key));
        op_string_key.type = type;
        op_string_key.range = range;
        fwrite(&op_string_key, sizeof(op_string_key), 1, out);
    } else {
        char *next;
        op_integer_key.key = strtoul(key, &next, 10);
        op_integer_key.type = type;
        op_integer_key.range = range;
        fwrite(&op_integer_key, sizeof(op_integer_key), 1, out);
    }

    if (tick-- == 0) {
        printf("\rfinished: %d ops", finished);
        fflush(stdout);
    }
    finished++;
}

int main(int argc, char **argv) {
    /*
     * Usage: ./compressor out_file in_file is_str_key
     *
     * Output data format: Array of struct op_{string,integer}_key
     */

    if (argc < 4) {
        printf("Usage: ./compressor out_file in_file is_str_key");
        exit(1);
    }

    out = fopen(argv[1], "wb");
    in = fopen(argv[2], "r");
    is_str_key = atoi(argv[3]);

    printf("YCSB Benchmark compressor, %s -> %s, %s key\n", argv[2], argv[1], is_str_key ? "string" : "integer");

    while (fscanf(in, "%s", buf) != EOF) {
        if (!strcmp(buf, "INSERT")) {
            fscanf(in, "%s", buf);
            append(OP_INSERT, buf, 0);
        } else if (!strcmp(buf, "UPDATE")) {
            fscanf(in, "%s", buf);
            append(OP_UPDATE, buf, 0);
        } else if (!strcmp(buf, "READ")) {
            fscanf(in, "%s", buf);
            append(OP_READ, buf, 0);
        } else if (!strcmp(buf, "SCAN")) {
            int range;
            fscanf(in, "%s %d", buf, &range);
            append(OP_SCAN, buf, range);
        }
    }

    fclose(in);
    fclose(out);
}
