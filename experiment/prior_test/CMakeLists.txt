cmake_minimum_required(VERSION 3.21)
project(nvm_perf C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "-O3")

add_executable(nvm_perf main.c pool.c pool.h measure.h test.c)

target_link_libraries(nvm_perf pmem pthread)
