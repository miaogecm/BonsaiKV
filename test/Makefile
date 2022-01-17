CFLAGS = -I. -O0 ${RAND}
LDFLAGS = -lpthread -O0
CC = gcc-11
RM = rm

all: a

%.o: %.c
	$(CC) -g -c $< -o $@ $(CFLAGS)

a: main.o hp.o link_list.o hash_set.o  
	$(CC) -g $^ -o $@ $(LDFLAGS)

clean:
	$(RM) -f *.o
	$(RM) -f a