cmake_minimum_required(VERSION 3.10)
project(nvm_interleave_granularity C)

set(CMAKE_C_STANDARD 99)

add_executable(nvm_interleave_granularity main.c pool.c test.c)

target_link_libraries(nvm_interleave_granularity pmem pthread)
