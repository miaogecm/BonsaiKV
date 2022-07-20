#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "assert.h"

/* 
 * input, output, type, num_n
 *
 * type:
 * 0    load
 * 1    op
 * 
 * op code:
 * insert   0
 * update   1
 * read     2
 * scan     3
 */

#define MAX_LEN	2048
#define MAX_N	100000000
uint8_t op_codes[MAX_N];

static inline uint64_t atoul(const char* str) {
	uint64_t res = 0;
	int i;

	for (i = 0; i < strlen(str); i++) {
		res = res * 10 + str[i] - '0';
	}

	return res;
}

// fprintf(val_out, "\"%s\", \n", val);
static inline void print_by_char(FILE* stream, const char* str) {
	int i;

	fprintf(stream, "\"");
	for (i = 0; i < strlen(str); i++) {
		fprintf(stream, "\\x%o", str[i]);
	}
	fprintf(stream, "\", \n");
}

static inline uint8_t get_op_id(const char* str) {
	if (str[0] == 'I') {
		return 0;
	} else if (str[0] == 'U') {
		return 1;
	} else if (str[0] == 'R') {
		return 2;
	} else if (str[0] == 'S') {
		return 3;
	}

	assert(0);
}

char key[MAX_LEN];
char val[MAX_LEN];

int main(int argc, char* argv[]) {
	char* input_file, *output_file, *type, *num_n;
	char type_file[100];
	char key_file[100];
	char val_file[100];
	FILE *type_out, *key_out, *val_out;
	uint8_t op_id;

	char op[10];
	
	input_file = argv[1];
	output_file = argv[2];
	type = argv[3];
	num_n = argv[4];

	strcpy(type_file, output_file);
	strcat(type_file, "_type");
	strcpy(key_file, output_file);
	strcat(key_file, "_key");
	strcpy(val_file, output_file);
	strcat(val_file, "_val");

	freopen(output_file, "w", stdout);

	/* remove dir */
	printf("#include \"%s\"\n", &key_file[7]);
	printf("#include \"%s\"\n", &val_file[7]);

	freopen(input_file, "r", stdin);

	if (type[0] == '0') {
		key_out = fopen(key_file, "w");
#ifdef STR_KEY
		fprintf(key_out, "char load_k_arr[%s][%d] = {\n", num_n, KEY_LEN);
#else
		fprintf(key_out, "#include <stdint.h>\n\n");
		fprintf(key_out, "uint64_t load_k_arr[%s] = {\n", num_n);
#endif

		val_out = fopen(val_file, "w");
#ifdef STR_VAL
		fprintf(val_out, "char load_v_arr[%s][%d] = {\n", num_n, VAL_LEN);
#else
		fprintf(val_out, "#include <stdint.h>\n\n");
		fprintf(val_out, "uint64_t load_v_arr[%s] = {\n", num_n);
#endif
		while(scanf("%s %s %s", op, key, val) == 3) {
#ifdef STR_KEY
			fprintf(key_out, "\"%s\", \n", key);
#else
			fprintf(key_out, "%lu, \n", atoul(key));
#endif
#ifdef STR_VAL
			assert(strlen(val) <= VAL_LEN);
			print_by_char(val_out, val);
#else
			fprintf(val_out, "%lu, \n", atoul(val));
#endif
		}
		fprintf(key_out, "}; \n");
		fprintf(val_out, "}; \n");
	} else {
		key_out = fopen(key_file, "w");
#ifdef STR_KEY
		fprintf(key_out, "char op_k_arr[%s][%d] = {\n", num_n, KEY_LEN);
#else
		fprintf(key_out, "#include <stdint.h>\n\n");
		fprintf(key_out, "uint64_t op_k_arr[%s] = {\n", num_n);
#endif

		val_out = fopen(val_file, "w");
#ifdef STR_VAL
		fprintf(val_out, "char op_v_arr[%s][%d] = {\n", num_n, VAL_LEN);
#else
		fprintf(val_out, "#include <stdint.h>\n\n");
		fprintf(val_out, "uint64_t op_v_arr[%s] = {\n", num_n);
#endif
		type_out = fopen(type_file, "w");
		fprintf(type_out, "#include <stdint.h>\n\n");
		fprintf(type_out, "uint8_t type_arr[%s] = {\n", num_n);

		printf("#include \"%s\"\n", &type_file[7]);

		while(scanf("%s %s", op, key) == 2) {
			op_id = get_op_id(op);
			fprintf(type_out, "%d, ", op_id);
#ifdef STR_KEY
			fprintf(key_out, "\"%s\", \n", key);
#else
			fprintf(key_out, "%lu, \n", atoul(key));
#endif		
			strcpy(val, "0");
			if (op_id != 2) {
				scanf("%s", val);
			}
#ifdef STR_VAL
			assert(strlen(val) <= VAL_LEN);
			print_by_char(val_out, val);
#else
			fprintf(val_out, "%lu, \n", atoul(val));
#endif
		}
		fprintf(type_out, "}; \n");
		fprintf(key_out, "}; \n");
		fprintf(val_out, "}; \n");

		fclose(type_out);
	}

	fclose(stdin);
	fclose(stdout);
	fclose(key_out);
	fclose(val_out);

	return 0;
}