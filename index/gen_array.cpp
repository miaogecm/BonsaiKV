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

int main(int argc, char* argv[]) {
    char *input_file_name, *output_file_name, *type, *count, *thread_id, *thread_num;
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
        printf("uint64_t load_arr[%d][2] = {\n", size);
        while(cin >> op >> __arg1 >> __arg2) {
            printf("{%luUL, %luUL}, \n", __arg1, __arg2);
        }
        printf("};\n");
        
        fclose(stdin);
        fclose(stdout);
    } else {
        thread_id = argv[5];
        thread_num = argv[6];

        id = stoi(thread_id);
        num = stoi(thread_num);

        if (id == 1) {
            freopen(output_file_name, "w", stdout);
            printf("#include <stdint.h>\n\n");
            printf("uint64_t op_arr[%d][%d][3] = {\n", num, size);
        } else {
            freopen(output_file_name, "a", stdout);
        }

        printf("{\n");

        while(cin >> op >> __arg1) {
            if (op == "INSERT") {
                cin >> __arg2;
                printf("{%d, %luUL, %luUL}, \n", 0, __arg1, __arg2);
            } 
            else if (op == "UPDATE") {
                cin >> __arg2;
                printf("{%d, %luUL, %luUL}, \n", 0, __arg1, __arg2);
            }
            else if (op == "READ") {
                printf("{%d, %luUL}, \n", 2, __arg1);
            }
            else if (op == "SCAN") {
                cin >> __arg2;
                printf("{%d, %luUL, %luUL}, \n", 3, __arg1, __arg1 + __arg2 - 1);
            }
            else {
                perror("error: unkown op type!\n");
            }
        }

        printf("}, \n");

        if (id == num) {
            printf("};\n");
        }

        fclose(stdin);
        fclose(stdout);
    }

    return 0;
}