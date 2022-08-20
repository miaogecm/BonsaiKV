cmake_minimum_required(VERSION 3.22)
project(masstree_rmind C)

set(CMAKE_C_STANDARD 99)

add_library(masstree_dll SHARED adapter.c masstree.c)
