# Mini-KV Makefile -- CprE 308/3080 Project 2
#
# Targets:
#   make             build kvserver
#   make bench       build bench_client
#   make all         both
#   make clean       remove build artifacts
#   make tsan        build with ThreadSanitizer (Stage 3+ debugging)

CC      = gcc
CFLAGS  = -Wall -Wextra -Wpedantic -std=c11 -O2 -g -D_XOPEN_SOURCE=700 -pthread
LDFLAGS = -pthread

SERVER_SRCS = kvserver.c protocol.c table.c queue.c
BENCH_SRCS  = bench_client.c

SERVER_BIN = kvserver
BENCH_BIN  = bench_client

.PHONY: all bench clean tsan

all: $(SERVER_BIN) $(BENCH_BIN)

bench: $(BENCH_BIN)

$(SERVER_BIN): $(SERVER_SRCS) kv.h
	$(CC) $(CFLAGS) -o $@ $(SERVER_SRCS) $(LDFLAGS)

$(BENCH_BIN): $(BENCH_SRCS)
	$(CC) $(CFLAGS) -o $@ $(BENCH_SRCS) $(LDFLAGS)

# Use this during development of Stage 3 and 4 to catch data races.
# Run the server normally -- TSan reports races on stderr.
tsan: CFLAGS += -fsanitize=thread -O1
tsan: LDFLAGS += -fsanitize=thread
tsan: clean $(SERVER_BIN)

clean:
	rm -f $(SERVER_BIN) $(BENCH_BIN) *.o
