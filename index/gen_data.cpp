#include <bits/stdc++.h>
using namespace std;

/* 
 * in_file, out_file, type, count, thread_id, thread_num
 *
 * type:
 * 0    load
 * 1    op
 * 
 * output op:
 * insert   0
 * update   1
 * read     2
 * scan     3
 */

#define KEY_MAX_LEN     100

int main(int argc, char* argv[]) {
    char *input_file_name, *output_file_name, *type, *count;
    int size, id, num;

    string op;
    uint64_t __arg1, __arg2;

    input_file_name = argv[1];
    output_file_name = argv[2];
    type = argv[3];
    count = argv[4];

    freopen(input_file_name, "r", stdin);

    size = stoi(count);

    if (type[0] == '0') {
        freopen(output_file_name, "w", stdout);
        printf("#include <stdint.h>\n\n");
#ifdef LONG_KEY
        printf("char load_arr[%d][2][%d] = {\n", size, KEY_MAX_LEN);
#else
        printf("uint64_t load_arr[%d][2] = {\n", size);
#endif
        while(cin >> op >> __arg1 >> __arg2) {
#ifdef LONG_KEY
        printf("{\"%lu\", \"%lu\"}, \n", __arg1, __arg2);
#else
        printf("{%luUL, %luUL}, \n", __arg1, __arg2);
#endif
        }
        printf("};\n");
        
        fclose(stdin);
        fclose(stdout);
    } else {
        freopen(output_file_name, "w", stdout);
        printf("#include <stdint.h>\n\n");
#ifdef LONG_KEY
        printf("char op_arr[%d][3][%d] = {\n", size, KEY_MAX_LEN);
#else
        printf("uint64_t op_arr[%d][3] = {\n", size);
#endif
        while(cin >> op >> __arg1) {
            if (op == "INSERT") {
                cin >> __arg2;
#ifdef LONG_KEY
                printf("{\"%d\", \"%lu\", \"%lu\"}, \n", 0, __arg1, __arg2);
#else
                printf("{%d, %luUL, %luUL}, \n", 0, __arg1, __arg2);
#endif
            } 
            else if (op == "UPDATE") {
                cin >> __arg2;
#ifdef LONG_KEY
                printf("{\"%d\", \"%lu\", \"%lu\"}, \n", 0, __arg1, __arg2);
#else
                printf("{%d, %luUL, %luUL}, \n", 0, __arg1, __arg2);
#endif
            }
            else if (op == "READ") {
#ifdef LONG_KEY
                printf("{\"%d\", \"%lu\"}, \n", 2, __arg1);
#else
                printf("{%d, %luUL}, \n", 2, __arg1);
#endif
            }
            else if (op == "SCAN") {
                cin >> __arg2;
#ifdef LONG_KEY
                printf("{\"%d\", \"%lu\", \"%lu\"}, \n", 3, __arg1, __arg1 + __arg2 - 1);
#else
                printf("{%d, %luUL, %luUL}, \n", 3, __arg1, __arg1 + __arg2 - 1);
#endif
            }
            else {
                perror("error: unkown op type!\n");
            }
        }

        printf("}; \n");

        fclose(stdin);
        fclose(stdout);
    }

    return 0;
}