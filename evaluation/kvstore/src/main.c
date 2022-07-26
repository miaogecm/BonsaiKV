void run_tatp(const char *kvlib, int num_work_);
void run_tpcc(const char *kvlib, int num_w_, int num_work_);
void run_ycsb(const char *kvlib, int str_key_, int str_val_);

int main() {
    run_ycsb("/home/hhusjr/Projects/bonsai/evaluation/kvstore/lib/listdb/cmake-build-debug/liblistdb_dll.so", 0, 0);
}