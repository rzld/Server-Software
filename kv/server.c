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
#include <stdbool.h>

#define NTHREADS 4
#define BACKLOG 10

/* Add anything you want here. */
pthread_mutex_t lock;
pthread_cond_t newClient;
bool checkClient;
int workerBusy, clientWaiting, nConnections;
bool workerTaken[NTHREADS];
int run;

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {
  int socket_ = *(int*) p;
  int inputSize;
  char *message,
       buffer[255];

  printf("Worker %d is waiting for connection.\n", socket_);

  //lock thread
  pthread_mutex_lock(&lock); //Check error!!!

  workerTaken[socket_] = false;

  //wait for connection
  while(!checkClient)
  {
    pthread_cond_wait(&newClient, &lock);
  }

  workerTaken[socket_] = true;
  printf("Worker %d executing task.\n", socket_);

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
    printf("Failed.\n");
  }

  //parse the buffer
  int msg = parse_d(buffer, &cmd, &key, &text);

  //write response line
  if (cmd == D_PUT)
  {
    //the line contains "put key value" command
    // the pointers key and text point to 0-terminated strings
    // containing the key and value
    int put_ = createItem(key, text);
  }
  else if (cmd == D_GET)
  {
    //contains a "get key" command
    // pointer key points to the key and text is null
    int get_ = findValue(key);
  }
  else if (cmd == D_COUNT)
  {
    //contains "count" command. key and value null
    int itemsCount = countItems();
  }
  else if (cmd == D_DELETE)
  {
    //contains "delete" key. pointer key points to the key and text is null
    int del_ = deleteItem(key, 1);
  }
  else if (cmd == D_EXISTS)
  {
    //contains "exists" key command. pointer key points to the key and text is nully
    int exists_ = itemExists(key);
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

  pthread_mutex_unlock(&lock);
}

/* You may add code to the main() function. */
int main(int argc, char** argv) {
    int cport, dport; /* control and data ports. */

	if (argc < 3) {
        printf("Usage: %s data-port control-port\n", argv[0]);
        exit(1);
	} else {
        cport = atoi(argv[1]);
        dport = atoi(argv[2]);
	}

  // start writing
  int controlSocket, dataSocket, clientSocket, workerID[NTHREADS];
  struct sockaddr_in controls, clients;
  pthread_t worker_thread[NTHREADS];

  pthread_mutex_init(&lock, NULL);  //dont forget error handling!

  //worker thread pools
  for (int i = 0; i < NTHREADS; i++)
  {
    workerID[i] = i;
    pthread_create(&worker_thread[i], NULL, worker, workerID[i]);
  }

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
  socklen_t len = sizeof(controls);
  memset(&controls, 0, len);
  controls.sin_family = AF_INET;
  controls.sin_addr.s_addr = htonl(INADDR_ANY);
  controls.sin_port = htons(cport);

  socklen_t len2 = sizeof(clients);
  memset(&clients, 0, len2);
  clients.sin_family = AF_INET;
  clients.sin_addr.sts_addr = htonl(INADDR_ANY);
  clients.sin_port = htons(dport);

  //bind to two sockets
  int errBindC = bind(controlSocket, &controls, len); //control socket
  int errBindD = bind(dataSocket, &clients, len);     //data socket
  if (errBindC < 0 || errBindD < 0)
  {
    printf("Bind error.\n");
    return 1;
  }
  printf("Bind success.\n");

  //listen
  listen(controlSocket, BACKLOG);
  listen(dataSocket, BACKLOG);

  //waiting for incoming connection
  printf("Waiting for clients...");
  int c = sizeof(struct sockaddr_in);

  //accept any incoming connection
  run = 1;
  while (run)
  {
    if (workerBusy < NTHREADS)
    {
      int conn = accept(dataSocket, &clients, &c);
      
      if (conn == -1)
      {
        //error
      }
      else
      {
        printf("Got a connection.");

        for (int i=0; i<NTHREADS; i++)
        {
          if (!workerTaken[i])
          {
            printf("Delegating to worker %d.\n", i);
            workerBusy++;

            //wake up thread, then join
            pthread_mutex_lock(&lock);              //check error!
            //broadcast or semaphore?
            pthread_cond_signal(&newClient);
            pthread_mutex_unlock(&lock);

            workerBusy--;
          }
        }
      }
    }
    else
    {
      clientWaiting++;
    }
  }

  for (int i=0; i<NTHREADS; i++)
  {
      pthread_join(worker_thread[i], NULL);   //check error!
  }

  return 0;
}
