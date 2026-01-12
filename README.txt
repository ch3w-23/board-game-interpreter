========================================================================
CSN6214 OPERATING SYSTEMS ASSIGNMENT
Concurrent Networked Board Game Interpreter (Connect 4)
========================================================================

1. PROJECT OVERVIEW
------------------------------------------------------------------------
This project implements a multiplayer Connect 4 server using a Hybrid 
Concurrency Architecture (Multiprocessing + Multithreading). The server 
hosts 3 to 5 players who connect via Inter-Process Communication (IPC).

2. SUPPORTED DEPLOYMENT MODE
------------------------------------------------------------------------
* Single-machine mode: Client-Server communication is handled via 
  IPC Named Pipes (FIFOs) located in /tmp/.

3. HOW TO COMPILE
------------------------------------------------------------------------
This project includes a Makefile for automated compilation.

Open a terminal in the project directory and run:

    make

To remove compiled binaries and clean up any stale pipe files (recommended
if the server crashes or before a fresh start):

    make clean

4. HOW TO RUN
------------------------------------------------------------------------
This game requires a minimum of 3 players and supports up to 5. You must 
open separate terminal windows for the server and each client.

Step 1: Start the Server (Terminal 1)
-------------------------------------
    ./server

Step 2: Connect Players (Terminals 2, 3, 4...)
----------------------------------------------
Open new terminal windows and run the client with a player name:

    ./client Alice
    ./client Bob
    ./client Charlie

Step 3: Start the Game
-------------------------------------
Once the minimum number of players (3) have connected, return to the 
Server Terminal (Terminal 1) and follow the prompt to begin the match.

5. GAME RULES SUMMARY
------------------------------------------------------------------------
* Objective: Be the first player to form a horizontal, vertical, or 
  diagonal line of 4 of your own tokens.
* Gameplay: 
  - The server assigns a unique token number (1-5) to each player.
  - Players take turns in a Round-Robin order.
  - On your turn, enter a column number (0-7) to drop your token.
  - The server enforces all rules and validates moves.

6. ARCHITECTURE HIGHLIGHTS
------------------------------------------------------------------------
* Hybrid Model: Uses fork() for client handling and pthreads for 
  internal server tasks (Scheduler, Logger).
* IPC: Uses named pipes (FIFOs) for message passing.
* Synchronization: Uses Process-Shared Mutexes to protect shared memory.
