# Compiler settings
CC = gcc
CFLAGS = -Wall -g

# Targets
all: server client

# Compile the Server (needs -pthread for threads)
server: server.c
	$(CC) $(CFLAGS) server.c -o server -pthread

# Compile the Client
client: client.c
	$(CC) $(CFLAGS) client.c -o client

# Clean up binaries and temporary pipe files

clean:
	rm -f server client
	rm -f /tmp/server_fifo /tmp/client_fifo_* /tmp/temp_fifo_*
