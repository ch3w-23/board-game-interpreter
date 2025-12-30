// client.c - Player connection code
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: ./client <player_number>\n");
        printf("Example: ./client 0   (for Player 1)\n");
        printf("         ./client 1   (for Player 2)\n");
        return 1;
    }
    
    int player_id = atoi(argv[1]);
    
    printf("=== Connect Four Client (Player %d) ===\n", player_id + 1);
    printf("Client PID: %d\n", getpid());
    
    // Create player-specific FIFO name
    char my_fifo[50];
    sprintf(my_fifo, "/tmp/client_fifo_%d", player_id);
    
    printf("[CONNECTING] Connecting to server...\n");
    
    // Connect to server FIFO
    int server_fd = open("/tmp/server_fifo", O_WRONLY);
    if (server_fd < 0) {
        perror("Cannot connect to server");
        printf("Make sure the server is running first!\n");
        return 1;
    }
    
    printf("[CONNECTED] Connected to server!\n");
    
    // Send ready message to server (keep server_fd open for moves)
    char ready_msg[100];
    sprintf(ready_msg, "PLAYER_%d_READY", player_id + 1);
    write(server_fd, ready_msg, strlen(ready_msg) + 1);
    
    printf("[READY] Sent ready message to server\n");
    
    // Open our own FIFO for receiving messages from server
    mkfifo(my_fifo, 0666);
    printf("[IPC] Opening client FIFO: %s\n", my_fifo);
    int my_fd = open(my_fifo, O_RDONLY);
    
    if (my_fd < 0) {
        perror("Failed to open client FIFO");
        return 1;
    }
    
    printf("[STATUS] Waiting for game to start...\n\n");
    
    // Wait for game messages
    while (1) {
        char buffer[256];
        
        // Blocking read - wait for messages from server
        int bytes_read = read(my_fd, buffer, sizeof(buffer));
        
        if (bytes_read > 0) {
            printf("[SERVER] %s\n", buffer);
            // If it's our turn, prompt and send move to server
            if (strncmp(buffer, "YOUR_TURN", 9) == 0) {
                char input[32];
                printf("Enter column (0-7): ");
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin) != NULL) {
                    int col = atoi(input);
                    char move_msg[100];
                    sprintf(move_msg, "PLAYER_%d_MOVE_%d", player_id + 1, col);
                    write(server_fd, move_msg, strlen(move_msg) + 1);
                    printf("[MOVE] Sent: %s\n", move_msg);
                }
            }
        } else if (bytes_read == 0) {
            printf("\n[DISCONNECTED] Server closed connection\n");
            break;
        } else {
            perror("Read error");
            break;
        }
    }
    
    close(my_fd);
    close(server_fd);
    return 0;
}