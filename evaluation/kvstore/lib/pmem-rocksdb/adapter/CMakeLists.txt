cmake_minimum_required(VERSION 3.22)
project(adapter CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "-DON_DCPMM -fno-rtti")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

add_library(rocksdb_dll SHARED adapter.cc)
target_link_libraries(rocksdb_dll rocksdb pmem pmemobj)
