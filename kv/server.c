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
  enum DATA_CMD cmd;    //data
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
  if (cmd == D_PUT)
  {
    //the line contains "put key value" command
    // the pointers key and text point to 0-terminated strings
    // containing the key and value
  }
  else if (cmd == D_GET)
  {
    //contains a "get key" command
    // pointer key points to the key and text is null
  }
  else if (cmd == D_COUNT)
  {
    //contains "count" command. key and value null
  }
  else if (cmd == D_DELETE)
  {
    //contains "delete" key. pointer key points to the key and text is null
  }
  else if (cmd == D_EXISTS)
  {
    //contains "exists" key command. pointer key points to the key and text is null
  }
  else if (cmd == D_END)
  {
    //line empty, close connection
  }
  else if (cmd == D_ERR_OL)
  {
    //error: line too long
  }
  else if (cmd == D_ERR_INVALID)
  {
    //error: invalid command
  }
  else if (msg == D_ERR_SHORT)
  {
    //error: too few parameters
  }
  else if (msg == D_ERR_LONG)
  {
    //error: too many parameters
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
  int controlSocket, dataSocket;
  struct sockaddr_in server, client;
  pthread_t worker[100], client[100];

  //create socket
  controlSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (controlSocket == -1)
  {
    printf("Control socket creation failed.\n");
  }

  dataSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (dataSocket == -1)
  {
    printf("Data socket creation failed.\n");
  }

  printf("Sockets created.\n");

  //sockaddr_in structure
  socklen_t len = sizeof(server);
  memset(&server, 0, len);
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = htonl(INADDR_ANY);
  server.sin_port = htons(dport);

  //bind
  int errBindC = bind(controlSocket, &server, len);
  int errBindD = bind(dataSocket, &server, len);

  if (errBindC < 0 || errBindD < 0)
  {
    printf("Bind error.\n");
    return 0;
  }
  printf("Bind success.\n");

  //listen
  listen(controlSocket, BACKLOG);
  listen(dataSocket, BACKLOG);

  //accept and incoming connection

  //thread
  for (int i = 0; i < NTHREADS; i++)
  {
    workerID[i] = i;
    pthread_create(&worker[i], NULL, worker, workerID[i]);
  }

    return 0;
}
