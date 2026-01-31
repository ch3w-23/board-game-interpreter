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
#include <poll.h>
#include <semaphore.h>
#include <stdarg.h>
#include <time.h>


// Define constants
#define MAX_PLAYERS 5
#define MIN_PLAYERS 3
#define BOARD_ROWS 8
#define BOARD_COLS 8
#define SHM_KEY 0x1234
#define GAME_ONGOING 0
#define GAME_FINISHED 1
#define MAX_NAME_LENGTH 20
#define LOG_MSG_MAX 256
#define LOG_QUEUE_CAP 256


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
    int last_move_row;                    // Last move's row
    int move_ready;                       // 1 when a move was applied for current turn
    sem_t move_sem;                       // signals scheduler that a valid move was applied
    char player_names[MAX_PLAYERS][MAX_NAME_LENGTH];  // Player names

    struct {
        char name[MAX_NAME_LENGTH];
        int wins;
    } scores[100]; // Store up to 100 distinct players
    int total_scores_stored;

    //  ADD LOGGER QUEUE INSIDE SHARED MEMORY
    char log_queue[LOG_QUEUE_CAP][LOG_MSG_MAX];
    int log_head;
    int log_tail;
    int log_count;

    //  ADD SEMAPHORES INSIDE SHARED MEMORY
    sem_t log_items;
    sem_t log_space;

} game_state_t;

// Global variables
game_state_t *shared_game_state = NULL;

int last_move_row; // Last move row

// ---- Logger queue (shared so fork()ed children can enqueue safely) ----
char log_queue[LOG_QUEUE_CAP][LOG_MSG_MAX];
int log_head;
int log_tail;
int log_count;

// Semaphores for producer/consumer logger queue
sem_t log_items;  // counts available log messages
sem_t log_space;  // counts free slots

int shmid;
pthread_mutex_t *game_mutex;
pthread_mutex_t *log_mutex;
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
int check_draw();
void reset_game();
void *logger_thread(void *arg);
void log_event(const char *fmt, ...);
int is_valid_move(int col);
int get_column_height(int col);
void print_game_statistics();



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
    shared_game_state->last_move_row = -1;

    // Initialize logger queue
    shared_game_state->log_head = 0;
    shared_game_state->log_tail = 0;
    shared_game_state->log_count = 0;

    // Initialize process-shared semaphores for logger (pshared=1)
    // (Safe here: before fork())
    sem_init(&shared_game_state->log_items, 1, 0);
    sem_init(&shared_game_state->log_space, 1, LOG_QUEUE_CAP);
    // Scheduler wait semaphore (pshared=1, initial=0)
    sem_init(&shared_game_state->move_sem, 1, 0);

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
    // Allocate shared memory for mutexes
    int game_mutex_shmid = shmget(SHM_KEY + 1, sizeof(pthread_mutex_t), IPC_CREAT | 0666);
    if (game_mutex_shmid < 0) {
        perror("shmget for game mutex failed");
        exit(1);
    }

    int log_mutex_shmid = shmget(SHM_KEY + 2, sizeof(pthread_mutex_t), IPC_CREAT | 0666);
    if (log_mutex_shmid < 0) {
        perror("shmget for log mutex failed");
        exit(1);
    }

    game_mutex = (pthread_mutex_t *)shmat(game_mutex_shmid, NULL, 0);
    if (game_mutex == (void *)-1) {
        perror("shmat for game mutex failed");
        exit(1);
    }

    log_mutex = (pthread_mutex_t *)shmat(log_mutex_shmid, NULL, 0);
    if (log_mutex == (void *)-1) {
        perror("shmat for log mutex failed");
        exit(1);
    }

    // Process-shared mutex attribute
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

    pthread_mutex_init(game_mutex, &attr);
    pthread_mutex_init(log_mutex, &attr);

    printf("Process-shared mutexes (game + logger) initialized\n");
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
    (void)sig;
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        int player_idx = -1;
        int active_count = 0;
        char pname[MAX_NAME_LENGTH] = "UNKNOWN";

        pthread_mutex_lock(game_mutex);

        // find which player died + mark inactive
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (shared_game_state->player_pids[i] == pid) {
                shared_game_state->active_players[i] = 0;
                player_idx = i;
            }
        }

        // recompute active_count
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (shared_game_state->active_players[i] == 1) active_count++;
        }

        // copy name if we found the player
        if (player_idx != -1) {
            strncpy(pname, shared_game_state->player_names[player_idx], MAX_NAME_LENGTH);
            pname[MAX_NAME_LENGTH - 1] = '\0';
        }

        pthread_mutex_unlock(game_mutex);

        if (player_idx != -1) {
            printf("\n[ALERT] Player %d disconnected! (PID: %d)\n", player_idx + 1, pid);
            printf("[STATUS] Active players remaining: %d\n", active_count);
            fflush(stdout);

            log_event("LEAVE player=%d name=%s pid=%d", player_idx + 1, pname, pid);
        }
    }

    // auto-shutdown logic
    pthread_mutex_lock(game_mutex);
    int remaining = 0;
    int total_joined = shared_game_state->player_count;
    for (int i = 0; i < MAX_PLAYERS; i++) {
        if (shared_game_state->active_players[i]) remaining++;
    }
    pthread_mutex_unlock(game_mutex);

    if (total_joined > 0 && remaining == 0) {
        printf("\n[SERVER] All players disconnected. Server shutting down safely.\n");
        cleanup(0);
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
        char pname[MAX_NAME_LENGTH];
        strncpy(pname, shared_game_state->player_names[player_id], MAX_NAME_LENGTH);
        pname[MAX_NAME_LENGTH - 1] = '\0';
        pthread_mutex_unlock(game_mutex);

        printf("Player %d connected (PID: %d)\n", player_id + 1, pid);

        //  ADD join logging 
        log_event("JOIN player=%d name=%s pid=%d", player_id + 1, pname, pid);
    }
}

void monitor_and_broadcast_updates(int player_id, int client_fd) {
    int last_seen_move = -1;
    struct pollfd pfd;

    // Set up polling to check for errors/disconnection
    pfd.fd = client_fd;
    pfd.events = POLLERR | POLLHUP; // We care about errors or hang-ups
   
    while (1) {
        // 1. Connection Health Check
        pfd.revents = 0; // Reset events
        // Check status (timeout 0 means don't wait, just check instantly)
        if (poll(&pfd, 1, 0) > 0) {
            if (pfd.revents & (POLLERR | POLLHUP)) {
                printf("[CHILD] Detected disconnect for Player %d. Exiting handler.\n", player_id + 1);
                break; // Exit loop -> Child process dies -> SIGCHLD sent to Parent
            }
        }

        // 2. Board Update Check (Your original logic)
        pthread_mutex_lock(game_mutex);
        int current_move_count = shared_game_state->move_count;
        pthread_mutex_unlock(game_mutex);
        
        if (current_move_count != last_seen_move) {
            last_seen_move = current_move_count;
            char board_msg[1024];
            format_board(board_msg, sizeof(board_msg));
            
            // Send update to client
            // If this write fails, we also know client is gone
            if (write(client_fd, board_msg, strlen(board_msg) + 1) < 0) {
                perror("Write to client FIFO failed");
                break; 
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
    log_event("GAME_START players=%d", player_count);
}

void start_game_threads(void) {
    pthread_t sched_thr, reader_thr, log_thr;
    pthread_create(&sched_thr, NULL, scheduler_thread, NULL);
    pthread_create(&reader_thr, NULL, server_reader_thread, NULL);
    pthread_create(&log_thr, NULL, logger_thread, NULL);

    pthread_detach(sched_thr);
    pthread_detach(reader_thr);
    pthread_detach(log_thr);
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

// Enqueue a formatted log message into the shared queue.
// Non-blocking: if the queue is full, the message is dropped.
void log_event(const char *fmt, ...) {
    char msg[LOG_MSG_MAX];

    // Timestamp
    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_now);

    // Format body
    char body[LOG_MSG_MAX];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof(body), fmt, ap);
    va_end(ap);

    snprintf(msg, sizeof(msg), "%s | %s", ts, body);

    // Reserve queue slot (non-blocking)
    if (sem_trywait(&shared_game_state->log_space) != 0) {
        return; // full -> drop
    }

    pthread_mutex_lock(log_mutex);
    strncpy(shared_game_state->log_queue[shared_game_state->log_tail], msg, LOG_MSG_MAX - 1);
    shared_game_state->log_queue[shared_game_state->log_tail][LOG_MSG_MAX - 1] = '\0';
    shared_game_state->log_tail = (shared_game_state->log_tail + 1) % LOG_QUEUE_CAP;
    if (shared_game_state->log_count < LOG_QUEUE_CAP) shared_game_state->log_count++;
    pthread_mutex_unlock(log_mutex);

    sem_post(&shared_game_state->log_items);
}

// Logger thread: consumes queue and writes ordered, non-interleaved logs to game.log
void *logger_thread(void *arg) {
    (void)arg;

    FILE *fp = fopen("game.log", "a");
    if (!fp) perror("Failed to open game.log");

    while (1) {
        sem_wait(&shared_game_state->log_items);

        char line[LOG_MSG_MAX];
        line[0] = '\0';

        pthread_mutex_lock(log_mutex);
        if (shared_game_state->log_count > 0) {
            strncpy(line, shared_game_state->log_queue[shared_game_state->log_head], LOG_MSG_MAX - 1);
            line[LOG_MSG_MAX - 1] = '\0';
            shared_game_state->log_head = (shared_game_state->log_head + 1) % LOG_QUEUE_CAP;
            shared_game_state->log_count--;
        }
        pthread_mutex_unlock(log_mutex);

        sem_post(&shared_game_state->log_space);

        if (fp && line[0]) {
            fprintf(fp, "%s\n", line);
            fflush(fp);
        }
    }
    return NULL;
}


void *scheduler_thread(void *arg) {
    (void)arg;
    int current = 0;

    while (1) {
        // --- CHECK ACTIVE PLAYER COUNT SAFELY ---
        pthread_mutex_lock(game_mutex);
        int active_count = 0;
        int total_joined = shared_game_state->player_count;
        for (int i = 0; i < MAX_PLAYERS; i++) {
            if (shared_game_state->active_players[i]) active_count++;
        }

        // ABORT GAME if below minimum
        if (active_count < MIN_PLAYERS) {
            if (shared_game_state->game_state != GAME_FINISHED) {
                printf("\n══════════════════════════════════════════\n");
                printf("[GAME ABORTED] Player count (%d) below minimum (%d).\n", active_count, MIN_PLAYERS);
                printf("[GAME ABORTED] Not enough players to continue. Ending session.\n");
                printf("══════════════════════════════════════════\n");
                fflush(stdout);

                char msg[] = "GAME OVER: Not enough players! Session aborted.";
                for (int i = 0; i < MAX_PLAYERS; i++) {
                    if (shared_game_state->active_players[i]) {
                        char fifo_name[50];
                        sprintf(fifo_name, "/tmp/client_fifo_%d", i);
                        int fd = open(fifo_name, O_WRONLY | O_NONBLOCK);
                        if (fd >= 0) {
                            write(fd, msg, strlen(msg) + 1);
                            close(fd);
                        }
                    }
                }

                shared_game_state->game_state = GAME_FINISHED;
                reset_game();
            }
            pthread_mutex_unlock(game_mutex);

            printf("[SCHEDULER] Game ended. Scheduler thread exiting.\n");
            return NULL;
        }

        // If game was finished but enough players again (optional resume)
        if (shared_game_state->game_state == GAME_FINISHED && active_count >= MIN_PLAYERS) {
            shared_game_state->game_state = GAME_ONGOING;
            printf("[GAME RESUME] Enough players connected. Game continuing!\n");
        }

        pthread_mutex_unlock(game_mutex);
        // --- END ACTIVE COUNT CHECK ---

        // 1) Pick next active player (Round Robin)
        pthread_mutex_lock(game_mutex);

        int found = 0;
        int chosen = -1;

        for (int k = 0; k < MAX_PLAYERS; k++) {
            int idx = (current + k) % MAX_PLAYERS;
            if (idx < shared_game_state->player_count && shared_game_state->active_players[idx]) {
                chosen = idx;
                shared_game_state->current_player = idx;
                shared_game_state->move_ready = 0;
                current = (idx + 1) % MAX_PLAYERS;
                found = 1;
                break;
            }
        }

        char pname[MAX_NAME_LENGTH] = "UNKNOWN";
        if (chosen != -1) {
            strncpy(pname, shared_game_state->player_names[chosen], MAX_NAME_LENGTH);
            pname[MAX_NAME_LENGTH - 1] = '\0';
        }

        pthread_mutex_unlock(game_mutex);

        if (!found) {
            usleep(100000);
            continue;
        }

        // 2) Log turn
        log_event("TURN player=%d name=%s", chosen + 1, pname);

        // 3) Prompt current player
        char prompt[128];
        pthread_mutex_lock(game_mutex);
        snprintf(prompt, sizeof(prompt), "YOUR_TURN\n%s, enter column (0-7):",
                 shared_game_state->player_names[shared_game_state->current_player]);
        pthread_mutex_unlock(game_mutex);

        send_to_client(chosen, prompt);

        // 4) Wait for the move (no busy-wait)
        while (1) {
            // block until a move is applied
            sem_wait(&shared_game_state->move_sem);

            pthread_mutex_lock(game_mutex);

            // If player disconnected, abandon this wait and choose next player
            if (shared_game_state->active_players[shared_game_state->current_player] == 0) {
                pthread_mutex_unlock(game_mutex);
                break; // go pick next active player
            }

            // If move is ready, we can proceed to win/draw check
            if (shared_game_state->move_ready == 1) {
                pthread_mutex_unlock(game_mutex);
                break;
            }

            pthread_mutex_unlock(game_mutex);
        }

        // If player disconnected during turn, continue loop
        pthread_mutex_lock(game_mutex);
        if (shared_game_state->active_players[shared_game_state->current_player] == 0) {
            pthread_mutex_unlock(game_mutex);
            continue;
        }
        pthread_mutex_unlock(game_mutex);

        // 5) Check for win/draw
        pthread_mutex_lock(game_mutex);

        int won = check_win();
        if (won) {
            int winner_idx = shared_game_state->last_move_player;
            char *winner_name = shared_game_state->player_names[winner_idx];

            printf("WINNER: %s!\n", winner_name);
            update_score(winner_name);

            char win_msg[100];
            sprintf(win_msg, "GAME OVER! Winner is %s", winner_name);

            for (int i = 0; i < shared_game_state->player_count; i++)
                send_to_client(i, win_msg);

            sleep(5);
            reset_game();
            pthread_mutex_unlock(game_mutex);
            continue;
        }

        if (check_draw()) {
            log_event("GAME_DRAW");
            for (int i = 0; i < shared_game_state->player_count; i++)
                send_to_client(i, "GAME OVER! It's a DRAW!");
            sleep(5);
            reset_game();
            pthread_mutex_unlock(game_mutex);
            continue;
        }

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
    (void)arg;
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

                int ok = 0;
                int moved_row = -1;
                char pname[MAX_NAME_LENGTH] = "UNKNOWN";

                pthread_mutex_lock(game_mutex);

                if (player_num == shared_game_state->current_player && shared_game_state->active_players[player_num]) {
                    ok = apply_move(player_num, col);
                    if (ok) {
                        shared_game_state->move_ready = 1;
                        moved_row = shared_game_state->last_move_row;
                        strncpy(pname, shared_game_state->player_names[player_num], MAX_NAME_LENGTH);
                        pname[MAX_NAME_LENGTH - 1] = '\0';

                        // Wake scheduler immediately (no busy-wait needed)
                        sem_post(&shared_game_state->move_sem);
                    }
                }

                pthread_mutex_unlock(game_mutex);

                if (ok) {
                    log_event("MOVE player=%d name=%s col=%d row=%d", player_num + 1, pname, col, moved_row);
                } else {
                    send_to_client(player_num, "INVALID MOVE. Try again.");
                    
                    // Resend the turn prompt so the client unlocks
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
    }
    return NULL;
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
        shared_game_state->scores[idx].name[MAX_NAME_LENGTH - 1] = '\0';
        shared_game_state->scores[idx].wins = 1;
        shared_game_state->total_scores_stored++;
    }
    save_scores(); // Save immediately after update
}

// Reset the board for the next game
void reset_game() {
    pthread_mutex_lock(game_mutex);

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
    shared_game_state->last_move_player = -1;
    shared_game_state->last_move_col = -1;
    shared_game_state->last_move_row = -1;

    int pc = shared_game_state->player_count;
    pthread_mutex_unlock(game_mutex);

    log_event("GAME_RESET");

    for (int i = 0; i < pc; i++) {
        pthread_mutex_lock(game_mutex);
        int active = shared_game_state->active_players[i];
        pthread_mutex_unlock(game_mutex);

        if (active) send_to_client(i, "GAME RESET! New game starting...");
    }
}

/*
 * ============================================================================
 * GAME LOGIC IMPLEMENTATION
 * Below are the improved game logic functions for Connect Four
 * ============================================================================
 */

/*
 * FUNCTION: format_board
 * PURPOSE: Renders the game board as a formatted string for display to clients
 */
void format_board(char *out, size_t out_size) {
    char *p = out;
    size_t remain = out_size;
    int wrote;
    
    // Header with move count
    wrote = snprintf(p, remain, "\n╔═══════════════════════════════╗\n");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    wrote = snprintf(p, remain, "║    CONNECT FOUR - MOVE %2d    ║\n", 
                    shared_game_state->move_count);
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    wrote = snprintf(p, remain, "╚═══════════════════════════════╝\n");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    // Column numbers header
    wrote = snprintf(p, remain, "    0 1 2 3 4 5 6 7\n");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    // Top border
    wrote = snprintf(p, remain, "  ┌─────────────────┐\n");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    // Board rows
    for (int i = 0; i < BOARD_ROWS; i++) {
        wrote = snprintf(p, remain, "%d │", i);
        p += wrote; remain -= (remain > wrote ? wrote : remain);
        
        for (int j = 0; j < BOARD_COLS; j++) {
            char cell = shared_game_state->board[i][j];
            
            // Highlight the last move with brackets
            if (i == shared_game_state->last_move_row && 
                j == shared_game_state->last_move_col &&
                shared_game_state->last_move_player >= 0) {
                wrote = snprintf(p, remain, "[%c]", cell);
            } else {
                wrote = snprintf(p, remain, " %c ", cell);
            }
            p += wrote; remain -= (remain > wrote ? wrote : remain);
        }
        
        wrote = snprintf(p, remain, "│\n");
        p += wrote; remain -= (remain > wrote ? wrote : remain);
    }
    
    // Bottom border
    wrote = snprintf(p, remain, "  └─────────────────┘\n");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    // Legend
    wrote = snprintf(p, remain, "\n  Legend: ");
    p += wrote; remain -= (remain > wrote ? wrote : remain);
    
    for (int i = 0; i < shared_game_state->player_count; i++) {
        if (shared_game_state->active_players[i]) {
            wrote = snprintf(p, remain, "%c=%s ", 
                           '1' + i, 
                           shared_game_state->player_names[i]);
            p += wrote; remain -= (remain > wrote ? wrote : remain);
        }
    }
    
    snprintf(p, remain, "\n");
}

/*
 * FUNCTION: apply_move
 * PURPOSE: Applies a player's move to the board with gravity simulation
 */
int apply_move(int player_id, int col) {
    // Validation 1: Check column range
    if (col < 0 || col >= BOARD_COLS) {
        printf("[GAME LOGIC] Invalid move: Column %d out of range (0-%d)\n", 
               col, BOARD_COLS - 1);
        return 0;
    }
    
    // Validation 2: Check if column is full (top row occupied)
    if (shared_game_state->board[0][col] != '.') {
        printf("[GAME LOGIC] Invalid move: Column %d is full\n", col);
        return 0;
    }
    
    // Apply gravity: Find the lowest empty row in this column
    for (int row = BOARD_ROWS - 1; row >= 0; row--) {
        if (shared_game_state->board[row][col] == '.') {
            // Place the token
            shared_game_state->board[row][col] = '1' + player_id;
            
            // Update move tracking information
            shared_game_state->last_move_player = player_id;
            shared_game_state->last_move_col = col;
            shared_game_state->last_move_row = row;
            shared_game_state->move_count++;
            
            printf("[GAME LOGIC] Player %d placed token at [%d][%d]\n", 
                   player_id + 1, row, col);
            
            return 1;
        }
    }
    
    printf("[GAME LOGIC] Unexpected error: Column %d appears full\n", col);
    return 0;
}

/*
 * FUNCTION: check_direction
 * PURPOSE: Helper function to check for 4-in-a-row in a specific direction
 */
static int check_direction(int row, int col, int delta_row, int delta_col, char player_token) {
    char (*b)[BOARD_COLS] = shared_game_state->board;
    
    // Check if we can fit 4 tokens in this direction
    int end_row = row + (delta_row * 3);
    int end_col = col + (delta_col * 3);
    
    if (end_row < 0 || end_row >= BOARD_ROWS || 
        end_col < 0 || end_col >= BOARD_COLS) {
        return 0;
    }
    
    // Check all 4 positions
    for (int i = 0; i < 4; i++) {
        int check_row = row + (delta_row * i);
        int check_col = col + (delta_col * i);
        
        if (b[check_row][check_col] != player_token) {
            return 0;
        }
    }
    
    return 1;
}

/*
 * FUNCTION: check_win
 * PURPOSE: Checks if the last move resulted in a win condition
 */
int check_win() {
    int last_row = shared_game_state->last_move_row;
    int last_col = shared_game_state->last_move_col;
    
    if (last_row < 0 || last_row >= BOARD_ROWS || 
        last_col < 0 || last_col >= BOARD_COLS) {
        return 0;
    }
    
    char player_token = shared_game_state->board[last_row][last_col];
    
    if (player_token == '.') {
        return 0;
    }
    
    // Direction vectors: {delta_row, delta_col}
    int directions[4][2] = {
        {0, 1},   // Horizontal (right)
        {1, 0},   // Vertical (down)
        {1, 1},   // Diagonal (down-right)
        {1, -1}   // Anti-diagonal (down-left)
    };
    
    for (int dir = 0; dir < 4; dir++) {
        int dr = directions[dir][0];
        int dc = directions[dir][1];
        
        // Check starting from 3 positions back to the current position
        for (int offset = -3; offset <= 0; offset++) {
            int start_row = last_row + (dr * offset);
            int start_col = last_col + (dc * offset);
            
            if (start_row < 0 || start_row >= BOARD_ROWS || 
                start_col < 0 || start_col >= BOARD_COLS) {
                continue;
            }
            
            if (check_direction(start_row, start_col, dr, dc, player_token)) {
                printf("[GAME LOGIC] WIN DETECTED! Player %d has 4-in-a-row\n", 
                       shared_game_state->last_move_player + 1);
                printf("[GAME LOGIC] Direction: ");
                
                if (dr == 0 && dc == 1) printf("Horizontal\n");
                else if (dr == 1 && dc == 0) printf("Vertical\n");
                else if (dr == 1 && dc == 1) printf("Diagonal (\\)\n");
                else if (dr == 1 && dc == -1) printf("Anti-Diagonal (/)\n");
                
                return 1;
            }
        }
    }
    
    return 0;
}

/*
 * FUNCTION: check_draw
 * PURPOSE: Checks if the game is a draw (board full, no winner)
 */
int check_draw() {
    if (shared_game_state->move_count >= (BOARD_ROWS * BOARD_COLS)) {
        printf("[GAME LOGIC] DRAW! Board is full (64 moves)\n");
        return 1;
    }
    
    int top_row_full = 1;
    for (int col = 0; col < BOARD_COLS; col++) {
        if (shared_game_state->board[0][col] == '.') {
            top_row_full = 0;
            break;
        }
    }
    
    if (top_row_full) {
        printf("[GAME LOGIC] DRAW! All columns are full\n");
        return 1;
    }
    
    return 0;
}

/*
 * FUNCTION: is_valid_move
 * PURPOSE: Pre-validates a move without applying it
 */
int is_valid_move(int col) {
    if (col < 0 || col >= BOARD_COLS) {
        return 0;
    }
    
    if (shared_game_state->board[0][col] != '.') {
        return 0;
    }
    
    return 1;
}

/*
 * FUNCTION: get_column_height
 * PURPOSE: Returns how many tokens are in a specific column
 */
int get_column_height(int col) {
    if (col < 0 || col >= BOARD_COLS) {
        return -1;
    }
    
    int height = 0;
    for (int row = BOARD_ROWS - 1; row >= 0; row--) {
        if (shared_game_state->board[row][col] != '.') {
            height++;
        } else {
            break;
        }
    }
    
    return height;
}

/*
 * FUNCTION: print_game_statistics
 * PURPOSE: Prints useful game statistics for debugging
 */
void print_game_statistics() {
    printf("\n=== GAME STATISTICS ===\n");
    printf("Total moves: %d\n", shared_game_state->move_count);
    printf("Active players: ");
    
    int active_count = 0;
    for (int i = 0; i < shared_game_state->player_count; i++) {
        if (shared_game_state->active_players[i]) {
            printf("%s ", shared_game_state->player_names[i]);
            active_count++;
        }
    }
    printf("(%d total)\n", active_count);
    
    printf("\nColumn fullness:\n");
    for (int col = 0; col < BOARD_COLS; col++) {
        int height = get_column_height(col);
        printf("  Col %d: %d/8 tokens ", col, height);
        
        printf("[");
        for (int i = 0; i < 8; i++) {
            if (i < height) printf("█");
            else printf(" ");
        }
        printf("]\n");
    }
    
    if (shared_game_state->last_move_player >= 0) {
        printf("\nLast move: %s played column %d (row %d)\n",
               shared_game_state->player_names[shared_game_state->last_move_player],
               shared_game_state->last_move_col,
               shared_game_state->last_move_row);
    }
    
    printf("=======================\n\n");
}