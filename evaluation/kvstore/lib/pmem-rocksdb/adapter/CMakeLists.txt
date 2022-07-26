cmake_minimum_required(VERSION 3.22)
project(adapter CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "-DON_DCPMM")

add_executable(adapter adapter.cc)
target_link_libraries(adapter rocksdb pmem pmemobj)
