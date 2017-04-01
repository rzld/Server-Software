/* Server program for key-value store. */

//References:
// https://gist.github.com/batuhangoksu/2b3afe5970b262d54626

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
  int inputSize;
  char *message,
       buffer[255];

  //welcome message
  message = "Welcome to the KV store.\n";
  write(socket_, message, strlen(message));
  message = "What do you want to do?\n";
  write(socket_, message, strlen(message));

  //read from client
  enum DATA_CMD cmd;
  char* key;
  char* text;
  while ((inputSize = recv(socket_, buffer, 255, 0)))
  {
    // end of string marker
    buffer[inputSize] = '\0';
    // clear message buffer
    memset(buffer, 0, 255);
  }

  if (inputSize == 0)
  {
    printf("Disconnected.\n");
    fflush(stdout);
    close(socket_);
  }
  else if (inputSize == -1)
  {
    perror("Failed.\n");
  }

  //parse the buffer
  int msg = parse_d(buffer, &cmd, &key, &text);

  //write response line
  if (msg == NULL)
  {
    //close connection
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

  // start writing
  int socketDesc, clientSocket, c;
  struct sockaddr_in server, client;
  pthread_t worker[100], client[100];

  //create socket
  socketDesc = socket(AF_INET, SOCK_STREAM, 0);
  if (socketDesc == -1)
  {
    printf("Socket creation failed.\n");
  }
  printf("Socket created.\n");

  //sockaddr_in structure
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = INADDR_ANY;
  server.sin_port = htons(dport);

  //bind

  //listen

  //accept and incoming connection

  //thread
  for (int i = 0; i < NTHREADS; i++)
  {
    workerID[i] = i;
    pthread_create(&worker[i], NULL, worker, workerID[i]);
  }

    return 0;
}
