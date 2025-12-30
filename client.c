// client.c - Player connection code
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_NAME_LENGTH 20

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
    // Format: "REGISTER:<player_name>"
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
        // Format: "ASSIGNED:<player_id>"
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
    while (1) {
        char buffer[1024]; // Increased for board display
        
        // Blocking read - wait for messages from server
        int bytes_read = read(my_fd, buffer, sizeof(buffer));
        
        if (bytes_read > 0) {
            // Check if it's a board update (contains BOARD marker)
            if (strstr(buffer, "BOARD") != NULL) {
                // Clear screen for better display (optional)
                // printf("\033[H\033[J");  // Clear screen
                printf("\n%s\n", buffer);
            } else {
                printf("\n[SERVER] %s\n", buffer);
            }
            
            // If it's our turn, prompt and send move to server
            if (strncmp(buffer, "YOUR_TURN", 9) == 0) {
                char input[32];
                printf("\n%s, enter column (0-7): ", player_name);
                fflush(stdout);
                if (fgets(input, sizeof(input), stdin) != NULL) {
                    int col = atoi(input);
                    char move_msg[100];
                    // Use player_id (0-based) for move messages
                    sprintf(move_msg, "PLAYER_%d_MOVE_%d", player_id + 1, col);
                    write(server_fd, move_msg, strlen(move_msg) + 1);
                    printf("[%s] Move sent: column %d\n", player_name, col);
                }
            }
            
            // Check for game over
            if (strstr(buffer, "GAME OVER") != NULL || 
                strstr(buffer, "wins") != NULL) {
                printf("\nGame session ended.\n");
                break;
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