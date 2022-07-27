void run_tatp(const char *kvlib, int num_work_);
void run_tpcc(const char *kvlib, int num_w_, int num_work_);
void run_ycsb(const char *kvlib, int str_key_, int str_val_);

int main() {
    run_ycsb("../../src/libbonsai.so", 0, 0);
}