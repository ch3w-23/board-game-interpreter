// client.c - Player connection code
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_NAME_LENGTH 20

// Increased buffer size to handle large board/lobby strings without splitting
#define BUFFER_SIZE 4096 

int main(int argc, char *argv[]) {
    char player_name[MAX_NAME_LENGTH];
    
    if (argc < 2) {
        // If no name provided, prompt for one
        printf("Enter your player name (max %d chars): ", MAX_NAME_LENGTH - 1);
        if (fgets(player_name, sizeof(player_name), stdin) == NULL) {
            printf("Error reading name\n");
            return 1;
        }
        // Remove newline
        player_name[strcspn(player_name, "\n")] = '\0';
        
        // Check if name is empty
        if (strlen(player_name) == 0) {
            strcpy(player_name, "Anonymous");
        }
    } else {
        // Use provided name
        strncpy(player_name, argv[1], MAX_NAME_LENGTH - 1);
        player_name[MAX_NAME_LENGTH - 1] = '\0';
    }
    
    printf("\n=== Connect Four Client ===\n");
    printf("Player: %s\n", player_name);
    printf("Client PID: %d\n", getpid());
    
    printf("[CONNECTING] Connecting to server...\n");
    
    // Connect to server FIFO
    int server_fd = open("/tmp/server_fifo", O_WRONLY);
    if (server_fd < 0) {
        perror("Cannot connect to server");
        printf("Make sure the server is running first!\n");
        return 1;
    }
    
    printf("[CONNECTED] Connected to server!\n");
    
    // Send registration message to server
    char register_msg[100];
    snprintf(register_msg, sizeof(register_msg), "REGISTER:%s", player_name);
    write(server_fd, register_msg, strlen(register_msg) + 1);
    
    printf("[REGISTERED] Sent registration to server as '%s'\n", player_name);
    printf("[STATUS] Waiting for game to start...\n\n");
    
    // Wait for server to assign us a player ID and FIFO
    char my_fifo[50];
    int player_id = -1;
    
    // Create a temporary FIFO to receive our assigned ID
    char temp_fifo[50];
    sprintf(temp_fifo, "/tmp/temp_fifo_%d", getpid());
    mkfifo(temp_fifo, 0666);
    
    // Tell server about our temp FIFO
    char temp_msg[100];
    snprintf(temp_msg, sizeof(temp_msg), "TEMP_FIFO:%s", temp_fifo);
    write(server_fd, temp_msg, strlen(temp_msg) + 1);
    
    // Open temp FIFO to receive assigned player ID
    int temp_fd = open(temp_fifo, O_RDONLY);
    if (temp_fd < 0) {
        perror("Failed to open temp FIFO");
        return 1;
    }
    
    // Read assigned player ID from server
    char assign_buffer[100];
    int bytes = read(temp_fd, assign_buffer, sizeof(assign_buffer));
    if (bytes > 0) {
        if (sscanf(assign_buffer, "ASSIGNED:%d", &player_id) == 1) {
            printf("[ASSIGNED] Server assigned Player ID: %d\n", player_id + 1);
            
            // Create our actual client FIFO name
            sprintf(my_fifo, "/tmp/client_fifo_%d", player_id);
        }
    }
    close(temp_fd);
    unlink(temp_fifo); // Clean up temp FIFO
    
    if (player_id == -1) {
        printf("[ERROR] Failed to get player ID from server\n");
        close(server_fd);
        return 1;
    }
    
    // Open our assigned FIFO for receiving messages from server
    mkfifo(my_fifo, 0666);
    printf("[IPC] Opening client FIFO: %s\n", my_fifo);
    int my_fd = open(my_fifo, O_RDONLY);
    
    if (my_fd < 0) {
        perror("Failed to open client FIFO");
        close(server_fd);
        return 1;
    }
    
    // Main game loop
    char buffer[BUFFER_SIZE]; 
    while (1) {
        // Clear buffer to avoid garbage data
        memset(buffer, 0, sizeof(buffer));

        // Blocking read - wait for messages
        ssize_t bytes_read = read(my_fd, buffer, sizeof(buffer) - 1); // Leave room for \0
        
        if (bytes_read > 0) {
            buffer[bytes_read] = '\0'; // Force null-termination
            
            int offset = 0;
            // Loop through all messages in the buffer
            while (offset < bytes_read) {
                char *current_msg = buffer + offset;
                
                // Ensure we don't read past the buffer
                int msg_len = strlen(current_msg);
                if (offset + msg_len > bytes_read) break; 

                // 1. Check for Board Update
                if (strstr(current_msg, "BOARD") != NULL) {
                    printf("\n%s\n", current_msg);
                } 
                // 2. Check for Turn
                else if (strstr(current_msg, "YOUR_TURN") != NULL) {
                    char input[32];
                    
                    while (1) {
                        printf("\nYOUR_TURN\n%s, Enter column (0-7) or type 'quit' to leave: ", player_name);
                        fflush(stdout);
                        
                        memset(input, 0, sizeof(input));
                        if (fgets(input, sizeof(input), stdin) == NULL) break;

                        input[strcspn(input, "\n")] = 0; // Remove newline

                        if (strlen(input) > 0) break; // Only accept non-empty input
                    }

                    if (strcasecmp(input, "quit") == 0 || strcasecmp(input, "exit") == 0) {
                        printf("Quitting game. Goodbye!\n");
                        close(my_fd);
                        close(server_fd);
                        exit(0);
                    }

                    char *endptr;
                    long val = strtol(input, &endptr, 10);
                    int col = (int)val;

                    // Validation
                    if (input[0] == '\0' || *endptr != '\0') {
                        col = -1; 
                    }

                    char move_msg[64];
                    snprintf(move_msg, sizeof(move_msg), "PLAYER_%d_MOVE_%d", player_id + 1, col);
                    write(server_fd, move_msg, strlen(move_msg) + 1);
                    
                    if (col >= 0 && col <= 7) {
                        printf("[%s] Move sent: column %d\n", player_name, col);
                    } else {
                        printf("[%s] Invalid input, please try again.\n", player_name);
                    }
                }
                // 3. Check for Game Over
                else if (strstr(current_msg, "GAME OVER") != NULL || strstr(current_msg, "wins") != NULL) {
                    printf("\n[SERVER] %s\n", current_msg);

                    if (strstr(current_msg, "shutting down") != NULL || strstr(current_msg, "Not enough players") != NULL) {
                        printf("Game session aborted by server. Exiting.\n");
                        close(my_fd);
                        close(server_fd);
                        exit(0);
                    }
                    
                }
                // 4. Normal Message
                else {
                    printf("\n[SERVER] %s\n", current_msg);
                }

                // Jump to the next message
                offset += msg_len + 1;
            }
        } else if (bytes_read == 0) {
            // Server closed the connection
            printf("\n[ERROR] Server disconnected.\n");
            break;
        }
    }
    
    close(my_fd);
    close(server_fd);
    return 0;
}