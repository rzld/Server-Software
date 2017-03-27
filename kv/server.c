/* Server program for key-value store. */

#include "kv.h"
#include "parser.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define NTHREADS = 4;
#define BACKLOG = 10;

/* Add anything you want here. */
pthread_mutex_t lock;

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {
  int socket_ = *(int*) p;
  char *message,
       buffer[255];

  //welcome message
  message = "Welcome to the KV store.\n";
  write(socket_, message, strlen(message));

  //read from client
  enum DATA_CMD cmd;
  char* key;
  char* text;

  //parse the buffer
  int msg = parse_d(buffer, &cmd, &key, &text);

  //write response line
  if (msg == NULL)
  {
    //close connection
  }
}

void run()
{
  int newSocket;
  struct sockaddr_in server, client;
  pthread_t worker[100], client[100];

  for (int i = 0; i < NTHREADS; i++)
  {
    workerID[i] = i;
    pthread_create(&worker[i], NULL, worker, workerID[i]);
  }
}

/* You may add code to the main() function. */
int main(int argc, char** argv) {
    int cport, dport; /* control and data ports. */

	if (argc < 3) {
        printf("Usage: %s data-port control-port\n", argv[0]);
        exit(1);
	} else {
        cport = atoi(argv[2]);
        dport = atoi(argv[1]);
	}

  run();

    return 0;
}
