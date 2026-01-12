#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>


// Define constants
#define MAX_PLAYERS 5
#define MIN_PLAYERS 3
#define BOARD_ROWS 8
#define BOARD_COLS 8
#define SHM_KEY 0x1234
#define GAME_ONGOING 0
#define GAME_FINISHED 1
#define MAX_NAME_LENGTH 20

// Shared memory structure
typedef struct {
    char board[BOARD_ROWS][BOARD_COLS];  // Game board
    int current_player;                   // 0-based player index
    int game_state;                       // GAME_ONGOING/GAME_FINISHED
    int winner;                           // Winner player index
    int player_count;                     // Number of connected players
    int player_pids[MAX_PLAYERS];         // PIDs of child processes
    int active_players[MAX_PLAYERS];      // 1 if active, 0 if disconnected
    int move_count;                       // Number of moves applied
    int last_move_player;                 // Last move's player
    int last_move_col;                    // Last move's column
    int move_ready;                       // 1 when a move was applied for current turn
    char player_names[MAX_PLAYERS][MAX_NAME_LENGTH];  // Player names
    struct {
        char name[MAX_NAME_LENGTH];
        int wins;
    } scores[100]; // Store up to 100 distinct players
    int total_scores_stored;
} game_state_t;

// Global variables
game_state_t *shared_game_state = NULL;
int shmid;
pthread_mutex_t *game_mutex;
char *server_fifo = "/tmp/server_fifo";
char *client_fifos[MAX_PLAYERS];


// Function prototypes
void init_shared_memory();
void init_mutex();
void cleanup(int sig);
void sigchld_handler(int sig);
int wait_for_player_connection(int player_id);
void create_player_process(int player_id);
void monitor_and_broadcast_updates(int player_id, int client_fd);
void notify_clients_game_starting(int player_count);
void start_game_threads(void);
void send_to_client(int player_id, const char *msg);
void *scheduler_thread(void *arg);
void *server_reader_thread(void *arg);
int apply_move(int player_id, int col);
void format_board(char *out, size_t out_size);
void load_scores();
void save_scores();
void update_score(const char *winner_name);
int check_win();
void reset_game();


int main(int argc, char *argv[]) {
    printf("=== Connect Four Server Starting ===\n");
    
    // Set up signal handlers
    signal(SIGINT, cleanup);
    signal(SIGCHLD, sigchld_handler);
    
    // Initialize shared memory and synchronization
    init_shared_memory();
    init_mutex();
    load_scores();
    
    // Initialize game state
    pthread_mutex_lock(game_mutex);
    shared_game_state->player_count = 0;
    shared_game_state->current_player = 0;
    shared_game_state->game_state = GAME_ONGOING;
    shared_game_state->winner = -1;
    shared_game_state->move_count = 0;
    shared_game_state->last_move_player = -1;
    shared_game_state->last_move_col = -1;
    shared_game_state->move_ready = 0;
    
    // Initialize board
    for (int i = 0; i < BOARD_ROWS; i++) {
        for (int j = 0; j < BOARD_COLS; j++) {
            shared_game_state->board[i][j] = '.';
        }
    }
    
    // Initialize player arrays
    for (int i = 0; i < MAX_PLAYERS; i++) {
        shared_game_state->player_pids[i] = -1;
        shared_game_state->active_players[i] = 0;
    }
    pthread_mutex_unlock(game_mutex);
    
    // Main server loop
    printf("Waiting for players to connect...\n");
    printf("Need %d to %d players to start.\n", MIN_PLAYERS, MAX_PLAYERS);
    
    // Create server named pipes/FIFO for IPC Channel
    mkfifo("/tmp/server_fifo", 0666);
    printf("[SERVER FIFO] Created: /tmp/server_fifo\n"); // client connect to this FIFO 
    
    int player_id = 0;
    
    while (player_id < MAX_PLAYERS) {
        printf("Available player slots: %d/%d\n", 
               MAX_PLAYERS - player_id, MAX_PLAYERS);
        printf("Minimum needed to start: %d\n\n", MIN_PLAYERS);
        
        // Wait for a player to connect; retry same slot on failure
        if (!wait_for_player_connection(player_id)) {
            printf("[WARN] Handshake failed for Player %d; retrying same slot...\n", player_id + 1);
            continue;
        }

        // After connection, fork the player process
        create_player_process(player_id);
        
        player_id++;
        
        // Auto-start if max players reached
        if (player_id >= MAX_PLAYERS) {
            printf("\nMAXIMUM PLAYERS REACHED!\n");
            printf("We have %d players. Starting game!\n", 
                   shared_game_state->player_count);
            printf("\nSTARTING GAME WITH %d PLAYERS!\n", shared_game_state->player_count);
            notify_clients_game_starting(shared_game_state->player_count);
            start_game_threads();
            break;
        }
        
        // Allow manual start if we have minimum players
        if (shared_game_state->player_count >= MIN_PLAYERS) {
            printf("\nMINIMUM PLAYERS REACHED!\n");
            printf("We have %d players. Game can start!\n", 
                   shared_game_state->player_count);
            printf("Waiting for all players to be ready...\n");
            printf("(Run more clients or press Enter to start)\n");
            
            char choice[10];
            printf("\nStart game now? (y/n, or press Enter): ");
            fflush(stdout);
            
            // Blocking read for user input
            if (fgets(choice, sizeof(choice), stdin) != NULL) {
                if (choice[0] == 'y' || choice[0] == 'Y' || choice[0] == '\n') {
                    printf("\nSTARTING GAME WITH %d PLAYERS!\n", shared_game_state->player_count);
                    notify_clients_game_starting(shared_game_state->player_count);
                    start_game_threads();
                    break;
                } else if (choice[0] == 'n' || choice[0] == 'N') {
                    printf("Waiting for more players...\n");
                    continue;
                }
            }
        }
        
        printf("\n");
    }
    
    // Game loop would start here (Member 3's work)
    printf("\n[GAME READY] Game initialized with %d players\n", 
           shared_game_state->player_count);
    printf("[WAITING] For scheduler thread to start...\n");
    
    // Wait for children (in real implementation, this would be in background)
    while (1) {
        pause(); // Wait for signals
    }
    
    return 0;
}

void init_shared_memory() {
    // Create shared memory segment
    shmid = shmget(SHM_KEY, sizeof(game_state_t), IPC_CREAT | 0666); //(rw-rw-rw-) - read/write for all
    if (shmid < 0) {
        perror("shmget failed");
        exit(1);
    }
    
    // Attach shared memory
    shared_game_state = (game_state_t *)shmat(shmid, NULL, 0);
    if (shared_game_state == (void *)-1) {
        perror("shmat failed");
        exit(1);
    }
    
    printf("Shared memory initialized successfully\n");
}

void init_mutex() {
    // Allocate shared memory for mutex
    int mutex_shmid = shmget(SHM_KEY + 1, sizeof(pthread_mutex_t), IPC_CREAT | 0666);
    if (mutex_shmid < 0) {
        perror("shmget for mutex failed");
        exit(1);
    }
    
    game_mutex = (pthread_mutex_t *)shmat(mutex_shmid, NULL, 0);
    if (game_mutex == (void *)-1) {
        perror("shmat for mutex failed");
        exit(1);
    }
    
    // Initialize mutex attributes for process sharing
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED); //mutex usable across processes
    
    pthread_mutex_init(game_mutex, &attr);
    printf("Process-shared mutex initialized\n");
}

void cleanup(int sig) {
    printf("\nServer shutting down...\n");

    if (shared_game_state != NULL) {
        save_scores();
    }
    
    // Detach shared memory
    if (shared_game_state != NULL) {
        shmdt(shared_game_state);
    }
    
    // Destroy shared memory
    shmctl(shmid, IPC_RMID, NULL);
    
    printf("Cleanup completed. Goodbye!\n");
    exit(0);
}

void sigchld_handler(int sig) {
    int status;
    pid_t pid;
    
    // Reap all zombie children
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        printf("Child process %d terminated\n", pid);
        
        // Mark player as inactive
        pthread_mutex_lock(game_mutex);
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (shared_game_state->player_pids[i] == pid) {
                shared_game_state->active_players[i] = 0;
                printf("Player %d disconnected\n", i + 1);
                break;
            }
        }
        pthread_mutex_unlock(game_mutex);
    }
}

int wait_for_player_connection(int player_id) {
    char client_fifo_name[50];
    sprintf(client_fifo_name, "/tmp/client_fifo_%d", player_id);
    
    printf("\n[WAITING] Waiting for Player %d to connect...\n", player_id+1);
    
    // Create the client FIFO for server->client messages
    mkfifo(client_fifo_name, 0666);
    printf("[IPC] Created client FIFO\n");
    
    // Open server FIFO for reading client messages
    printf("[IPC] Waiting for message from Player %d on /tmp/server_fifo...\n", player_id + 1);
    int server_fd = open("/tmp/server_fifo", O_RDONLY);

    if (server_fd < 0) {
        perror("Failed to open server FIFO");
        return 0;
    }
    
    // Read the client's registration message
    char buffer[256];
    char player_name[MAX_NAME_LENGTH] = "Player";
    char temp_fifo[50] = "";
    int got_register = 0, got_temp = 0;

    // Block until we have both messages
    while (!got_register || !got_temp) {
        ssize_t bytes_read = read(server_fd, buffer, sizeof(buffer));

        if (bytes_read > 0) {
            int offset = 0;
            while (offset < bytes_read) {
                // Messages are written null-terminated; parse each chunk
                size_t msg_len = strnlen(buffer + offset, bytes_read - offset);
                if (msg_len == (size_t)(bytes_read - offset)) {
                    // No terminator found in remaining buffer; wait for more data
                    break;
                }

                const char *msg = buffer + offset;
                if (!got_register && strncmp(msg, "REGISTER:", 9) == 0) {
                    strncpy(player_name, msg + 9, MAX_NAME_LENGTH - 1);
                    player_name[MAX_NAME_LENGTH - 1] = '\0';
                    got_register = 1;
                } else if (!got_temp && strncmp(msg, "TEMP_FIFO:", 10) == 0) {
                    sscanf(msg + 10, "%49s", temp_fifo);
                    got_temp = 1;
                }

                offset += (int)msg_len + 1; // step past terminator
            }
        } else if (bytes_read == 0) {
            // Writer closed; reopen to keep waiting
            close(server_fd);
            server_fd = open("/tmp/server_fifo", O_RDONLY);
            if (server_fd < 0) {
                perror("Failed to reopen server FIFO");
                return 0;
            }
        } else if (errno == EINTR) {
            continue; // interrupted, retry
        } else {
            perror("Error reading server FIFO");
            close(server_fd);
            return 0;
        }
    }

    close(server_fd);

    // Store player name in shared memory
    pthread_mutex_lock(game_mutex);
    strncpy(shared_game_state->player_names[player_id],
            player_name, MAX_NAME_LENGTH);
    pthread_mutex_unlock(game_mutex);

    printf("═══════════════════════════════════════\n");
    printf("[SUCCESS] %s connected as Player %d!\n",
           player_name, player_id + 1);

    // Respond on client's temp FIFO with assigned player ID
    int temp_fd = open(temp_fifo, O_WRONLY);
    if (temp_fd >= 0) {
        char assign_msg[50];
        snprintf(assign_msg, sizeof(assign_msg), "ASSIGNED:%d", player_id);
        write(temp_fd, assign_msg, strlen(assign_msg) + 1);
        close(temp_fd);
    } else {
        perror("Failed to open temp FIFO");
        return 0;
    }

    return 1;
}

void create_player_process(int player_id) {
    pid_t pid = fork();
    
    if (pid < 0) {
        perror("fork failed");
        return;
    }
    
    if (pid == 0) {
        // Child process - server side player handler
        printf("Player %d handler (PID: %d) ready\n", player_id + 1, getpid());
        
        char client_fifo_name[50];
        sprintf(client_fifo_name, "/tmp/client_fifo_%d", player_id);
        
        // Open client FIFO for writing (server→client messages)
        int client_fd = open(client_fifo_name, O_WRONLY);
        
        if (client_fd < 0) {
            perror("FIFO open failed");
            exit(1);
        }
        
        // Send welcome message
        char buffer[256];
        sprintf(buffer, "Welcome Player %d! Game starting soon...", player_id + 1);
        write(client_fd, buffer, strlen(buffer) + 1);

        monitor_and_broadcast_updates(player_id, client_fd);
        
        close(client_fd);
        exit(0);
        
    } else {
        // Parent process
        pthread_mutex_lock(game_mutex);
        shared_game_state->player_pids[player_id] = pid;
        shared_game_state->active_players[player_id] = 1;
        shared_game_state->player_count++;
        pthread_mutex_unlock(game_mutex);
        
        printf("Player %d connected (PID: %d)", 
               player_id + 1, pid);
    }
}

void monitor_and_broadcast_updates(int player_id, int client_fd) {
    int last_seen_move = -1;
   
    while (1) {
        // Safely read the current move count from shared memory
        pthread_mutex_lock(game_mutex);
        int current_move_count = shared_game_state->move_count;
        pthread_mutex_unlock(game_mutex);
        
        // Check if game state has changed since last check
        if (current_move_count != last_seen_move) {
            last_seen_move = current_move_count;
            char board_msg[1024];
            format_board(board_msg, sizeof(board_msg));
            write(client_fd, board_msg, strlen(board_msg) + 1);
            // Send update to client
            if (write(client_fd, board_msg, strlen(board_msg) + 1) < 0) {
                // Client may have disconnected
                perror("Write to client FIFO failed");
                break;  // Exit monitoring loop
            }
        }
        usleep(100000); // 100ms polling interval
    }
}

void notify_clients_game_starting(int player_count) {
    for (int i = 0; i < player_count; i++) {
        char client_fifo[50];
        sprintf(client_fifo, "/tmp/client_fifo_%d", i);
        int fd = open(client_fifo, O_WRONLY | O_NONBLOCK);
        if (fd >= 0) {
            char msg[100];
            sprintf(msg, "🎮 GAME STARTING with %d players!", player_count);
            write(fd, msg, strlen(msg) + 1);
            close(fd);
        }
    }
}

void start_game_threads(void) {
    pthread_t sched_thr, reader_thr;
    pthread_create(&sched_thr, NULL, scheduler_thread, NULL);
    pthread_create(&reader_thr, NULL, server_reader_thread, NULL);
    pthread_detach(sched_thr);
    pthread_detach(reader_thr);
}

// Helper: send a message to a client's FIFO
void send_to_client(int player_id, const char *msg) {
    char fifo_name[50];
    sprintf(fifo_name, "/tmp/client_fifo_%d", player_id);
    int fd = open(fifo_name, O_WRONLY | O_NONBLOCK);
    if (fd >= 0) {
        write(fd, msg, strlen(msg) + 1);
        close(fd);
    }
}

// Thread: schedules turns and prompts players
void *scheduler_thread(void *arg) {
    int current = 0;
    while (1) {
        // [Safety Check] Stop if game is over
        pthread_mutex_lock(game_mutex);
        if (shared_game_state->player_count < MIN_PLAYERS) {
            pthread_mutex_unlock(game_mutex);
            sleep(1);
            continue;
        }
        
        // 1. Find next active player (Round Robin)
        int found = 0;
        for (int k = 0; k < MAX_PLAYERS; k++) {
            int idx = (current + k) % MAX_PLAYERS;
            if (idx < shared_game_state->player_count && shared_game_state->active_players[idx]) {
                shared_game_state->current_player = idx;
                shared_game_state->move_ready = 0;
                current = (idx + 1) % MAX_PLAYERS;
                found = 1;
                break;
            }
        }
        pthread_mutex_unlock(game_mutex);

        if (!found) continue;

        // 2. Prompt current player
        char prompt[128];
        snprintf(prompt, sizeof(prompt), "YOUR_TURN\n%s, enter column (0-7):",
                shared_game_state->player_names[shared_game_state->current_player]);
        send_to_client(shared_game_state->current_player, prompt);

        // 3. Wait for their move
        while (1) {
            pthread_mutex_lock(game_mutex);
            int ready = shared_game_state->move_ready;
            pthread_mutex_unlock(game_mutex);
            if (ready) break;
            usleep(100000); // Wait 100ms
        }

        // 4. Check for Win
        pthread_mutex_lock(game_mutex);
        int won = check_win();
        if (won) {
            // Who moved last? They are the winner.
            int winner_idx = shared_game_state->last_move_player;
            char *winner_name = shared_game_state->player_names[winner_idx];
            
            printf("WINNER: %s!\n", winner_name);
            update_score(winner_name); // Save to file
            
            char win_msg[100];
            sprintf(win_msg, "GAME OVER! Winner is %s", winner_name);
            
            // Broadcast win
            for (int i=0; i<shared_game_state->player_count; i++) 
                send_to_client(i, win_msg);
            
            // Reset for next game
            sleep(5); // Give them time to read
            reset_game();
            pthread_mutex_unlock(game_mutex);
            continue; // Start loop again
        }
        
        // Check for Draw (Board Full)
        if (shared_game_state->move_count >= 64) {
             printf("GAME DRAW!\n");
             for (int i=0; i<shared_game_state->player_count; i++) 
                send_to_client(i, "GAME OVER! It's a DRAW!");
             sleep(5);
             reset_game();
             pthread_mutex_unlock(game_mutex);
             continue;
        }
        
        // Broadcast move if no win
        char msg[128];
        snprintf(msg, sizeof(msg), "MOVE %s -> Col %d", 
                shared_game_state->player_names[shared_game_state->last_move_player], 
                shared_game_state->last_move_col);
        pthread_mutex_unlock(game_mutex);
        
        for (int i = 0; i < shared_game_state->player_count; i++) {
            if (shared_game_state->active_players[i]) send_to_client(i, msg);
        }
    }
    return NULL;
}

// Thread: reads client messages from server FIFO and applies moves
void *server_reader_thread(void *arg) {
    char buffer[256];
    while (1) {
        int fd = open(server_fifo, O_RDONLY);
        if (fd < 0) {
            perror("server_fifo open failed");
            sleep(1);
            continue;
        }

        ssize_t n = read(fd, buffer, sizeof(buffer) - 1);
        if (n > 0) {
            buffer[n] = '\0';
            int player_num = -1, col = -1;
            // Expected: PLAYER_<n>_MOVE_<col>
            if (sscanf(buffer, "PLAYER_%d_MOVE_%d", &player_num, &col) == 2) {
                player_num -= 1; // convert to 0-based
                pthread_mutex_lock(game_mutex);
                int ok = 0;
                if (player_num == shared_game_state->current_player && shared_game_state->active_players[player_num]) {
                    ok = apply_move(player_num, col);
                    if (ok) {
                        shared_game_state->move_ready = 1;
                    }
                }
                pthread_mutex_unlock(game_mutex);
                if (!ok) {
                    send_to_client(player_num, "INVALID MOVE. Try again.");
                    
                    // [FIX] Resend the turn prompt so the client unlocks!
                    char prompt[128];
                    pthread_mutex_lock(game_mutex);
                    snprintf(prompt, sizeof(prompt), "YOUR_TURN\n%s, enter column (0-7):",
                            shared_game_state->player_names[player_num]);
                    pthread_mutex_unlock(game_mutex);
                    send_to_client(player_num, prompt);
                }
            }
        }
        close(fd);
        // Loop: reopen FIFO to wait for next writer
    }
    return NULL;
}

// Helper: apply a move to the board; returns 1 on success
int apply_move(int player_id, int col) {
    if (col < 0 || col >= BOARD_COLS) return 0;
    // Find lowest empty row in the column
    for (int row = BOARD_ROWS - 1; row >= 0; row--) {
        if (shared_game_state->board[row][col] == '.') {
            // Use distinct char per player: '1'..'5'
            shared_game_state->board[row][col] = '1' + player_id;
            shared_game_state->last_move_player = player_id;
            shared_game_state->last_move_col = col;
            shared_game_state->move_count++;
            return 1;
        }
    }
    return 0; // Column full
}

// Helper: format the board into a printable string
void format_board(char *out, size_t out_size) {
    char *p = out;
    size_t remain = out_size;
    int wrote = snprintf(p, remain, "\nBOARD (moves: %d)\n", shared_game_state->move_count);
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    for (int i = 0; i < BOARD_ROWS; i++) {
        wrote = snprintf(p, remain, "|");
        p += wrote; remain -= (remain > wrote ? wrote : remain);
        for (int j = 0; j < BOARD_COLS; j++) {
            wrote = snprintf(p, remain, "%c", shared_game_state->board[i][j]);
            p += wrote; remain -= (remain > wrote ? wrote : remain);
        }
        wrote = snprintf(p, remain, "|\n");
        p += wrote; remain -= (remain > wrote ? wrote : remain);
    }
    snprintf(p, remain, "\n");
}

// Load scores from scores.txt into shared memory
void load_scores() {
    FILE *fp = fopen("scores.txt", "r");
    shared_game_state->total_scores_stored = 0;
    
    if (fp == NULL) {
        printf("[PERSISTENCE] No scores.txt found. Starting fresh.\n");
        return;
    }

    char name[MAX_NAME_LENGTH];
    int wins;
    while (fscanf(fp, "%s %d", name, &wins) == 2) {
        int idx = shared_game_state->total_scores_stored;
        if (idx < 100) {
            strncpy(shared_game_state->scores[idx].name, name, MAX_NAME_LENGTH);
            shared_game_state->scores[idx].wins = wins;
            shared_game_state->total_scores_stored++;
        }
    }
    fclose(fp);
    printf("[PERSISTENCE] Loaded %d scores from file.\n", shared_game_state->total_scores_stored);
}

// Save shared memory scores back to scores.txt
void save_scores() {
    FILE *fp = fopen("scores.txt", "w");
    if (fp == NULL) {
        perror("Failed to save scores");
        return;
    }
    
    for (int i = 0; i < shared_game_state->total_scores_stored; i++) {
        fprintf(fp, "%s %d\n", shared_game_state->scores[i].name, shared_game_state->scores[i].wins);
    }
    fclose(fp);
    printf("[PERSISTENCE] Scores saved to scores.txt.\n");
}

// Update the winner's score safely
void update_score(const char *winner_name) {
    int found = 0;
    // Check if player exists
    for (int i = 0; i < shared_game_state->total_scores_stored; i++) {
        if (strcmp(shared_game_state->scores[i].name, winner_name) == 0) {
            shared_game_state->scores[i].wins++;
            found = 1;
            break;
        }
    }
    // If new player, add them
    if (!found && shared_game_state->total_scores_stored < 100) {
        int idx = shared_game_state->total_scores_stored;
        strncpy(shared_game_state->scores[idx].name, winner_name, MAX_NAME_LENGTH);
        shared_game_state->scores[idx].wins = 1;
        shared_game_state->total_scores_stored++;
    }
    save_scores(); // Save immediately after update
}

// Check if someone has won (Horizontal, Vertical, Diagonal)
int check_win() {
    char (*b)[BOARD_COLS] = shared_game_state->board;
    // Iterate all cells
    for (int r=0; r<BOARD_ROWS; r++) {
        for (int c=0; c<BOARD_COLS; c++) {
            char p = b[r][c];
            if (p == '.') continue;

            // Check Horizontal (Right)
            if (c+3 < BOARD_COLS && p==b[r][c+1] && p==b[r][c+2] && p==b[r][c+3]) return 1;
            // Check Vertical (Down)
            if (r+3 < BOARD_ROWS && p==b[r+1][c] && p==b[r+2][c] && p==b[r+3][c]) return 1;
            // Check Diagonal (Down-Right)
            if (r+3 < BOARD_ROWS && c+3 < BOARD_COLS && p==b[r+1][c+1] && p==b[r+2][c+2] && p==b[r+3][c+3]) return 1;
            // Check Diagonal (Down-Left)
            if (r+3 < BOARD_ROWS && c-3 >= 0 && p==b[r+1][c-1] && p==b[r+2][c-2] && p==b[r+3][c-3]) return 1;
        }
    }
    return 0;
}

// Reset the board for the next game
void reset_game() {
    printf("[GAME] Resetting board for new game...\n");
    
    // Clear board
    for (int i = 0; i < BOARD_ROWS; i++) {
        for (int j = 0; j < BOARD_COLS; j++) {
            shared_game_state->board[i][j] = '.';
        }
    }
    
    // Reset game flags but KEEP players connected
    shared_game_state->move_count = 0;
    shared_game_state->game_state = GAME_ONGOING;
    shared_game_state->winner = -1;
    shared_game_state->move_ready = 0;
    
    // Notify everyone
    send_to_client(0, "GAME RESET! New game starting..."); 
    send_to_client(1, "GAME RESET! New game starting...");
    send_to_client(2, "GAME RESET! New game starting...");
}