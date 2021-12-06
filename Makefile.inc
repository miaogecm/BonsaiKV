PROJ_DIR  := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
INC_DIR   := $(PROJ_DIR)/include
LIB_DIR   := $(PROJ_DIR)/src
LIB++_DIR := $(PROJ_DIR)/lib++
UT_DIR    := $(PROJ_DIR)/unittest
BIN_DIR   := $(PROJ_DIR)/bin
POOL_DIR  := /mnt/pmem

TOOLS_DIR := $(PROJ_DIR)/script

AR = ar
CC = gcc
CXX = g++
MAKE = make

# -jN
ifeq ($(strip $(NJOB)), )
  NJOB := ${shell nproc}
endif
