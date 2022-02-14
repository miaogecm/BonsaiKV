.PHONY: all clean
.DEFAULT_GOAL := all

LIBS=-lrt -lm -lpthread -lkvdata
INCLUDES=-I./include
CFLAGS=-O3 -std=c++11 -g 

output = main

all: main

main: src/test.cpp
	g++ $(CFLAGS) -o main src/test.cpp $(LIBS)

clean: 
	rm $(output)
