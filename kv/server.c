/* Server program for key-value store. */

//References:
// https://gist.github.com/batuhangoksu/2b3afe5970b262d54626
// http://stackoverflow.com/questions/9488185/waking-up-individual-threads-instead-of-busy-wait-in-pthreads

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
pthread_mutex_t main_lock, thr_lock;
pthread_cond_t main_cond, thr_cond;
bool checkClient, workerAvailable;
int workerBusy, clientWaiting, nConnections;
bool workerTaken[NTHREADS];
int run, connectionID[100];
int totalWorker = NTHREADS;
int clientNumber, clientsWaiting;

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {
  int workerID_ = *(int*) p;
  int socket_;
  int inputSize;
  char *message,
       buffer[255];

  while(run)
  {
    printf("Worker %d is waiting for connection.\n", workerID_);
    checkClient = false;
    //lock thread
    // workerTaken[socket_] = false;
    pthread_mutex_lock(&main_lock);
    if (clientsWaiting > 0)
    {
      pthread_cond_signal(&main_cond);
      workerAvailable = true;
    }
    pthread_mutex_unlock(&main_lock);

    pthread_mutex_lock(&thr_lock); //Check error!!!
    //wait for connection
    while(!checkClient)
    {
      pthread_cond_wait(&thr_cond, &thr_lock);
    }

    socket_ = connectionID[clientNumber];

    pthread_mutex_unlock(&thr_lock);
    //workerTaken[socket_] = true;

    totalWorker--;
    printf("Worker %d executing task.\n", workerID_);
    printf(">> Current available worker: %d\n\n", totalWorker);

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
      totalWorker++;
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

  }
}

/* You may add code to the main() function. */
int main(int argc, char** argv) {
    int cport, dport; /* control and data ports. */

	if (argc < 3) {
        printf("Usage: %s control-port data-port\n", argv[0]);
        exit(1);
	} else {
        cport = atoi(argv[1]);
        dport = atoi(argv[2]);
	}

  // start writing
  int controlSocket, dataSocket, clientSocket, workerID[NTHREADS];
  struct sockaddr_in controls, clients;
  pthread_t worker_thread[NTHREADS];

  pthread_mutex_init(&main_lock, NULL);  //dont forget error handling!
  pthread_mutex_init(&thr_lock, NULL);
  pthread_cond_init(&main_cond, NULL);
  pthread_cond_init(&thr_cond, NULL);

  //create socket
  controlSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (controlSocket == -1)
  {
    printf("Control socket creation failed.\n");
    return 1;
  }

  dataSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (dataSocket == -1)
  {
    printf("Data socket creation failed.\n");
    return 1;
  }

  printf("Sockets created.\n");

  checkClient = false;
  run = 1;

  //worker thread pools
  for (int i = 0; i < NTHREADS; i++)
  {
    workerID[i] = i;
    pthread_create(&worker_thread[i], NULL, worker, (void*)&workerID[i]);
  }

  sleep(1);

  //sockaddr_in structure
  socklen_t len = sizeof(controls);
  memset(&controls, 0, len);
  controls.sin_family = AF_INET;
  controls.sin_addr.s_addr = htonl(INADDR_ANY);
  controls.sin_port = htons(cport);

  socklen_t len2 = sizeof(clients);
  memset(&clients, 0, len2);
  clients.sin_family = AF_INET;
  clients.sin_addr.s_addr = htonl(INADDR_ANY);
  clients.sin_port = htons(dport);

  //bind to two sockets
  int errBindC = bind(controlSocket, (struct sockaddr *)&controls, len); //control socket
  int errBindD = bind(dataSocket, (struct sockaddr *)&clients, len2);     //data socket
  if (errBindC < 0 || errBindD < 0)
  {
    printf("Bind error.\n");
    return 1;
  }
  printf("Bind success.\n");

  //listen
  //listen(controlSocket, BACKLOG);
  listen(dataSocket, BACKLOG);

  //waiting for incoming connection
  printf("Waiting for clients...\n");
  workerAvailable = true;

  int c = sizeof(struct sockaddr_in);

  //accept any incoming connection
  while (run)
  {
    //if (workerBusy < NTHREADS)
    //{
      int conn = accept(dataSocket, (struct sockaddr *)&clients, (socklen_t*)&c);

      if (conn == -1)
      {
        //error
      }
      else
      {
        printf("> Available worker: %d.\n", totalWorker);

        if (totalWorker == 0)
        {
          workerAvailable = false;
          clientsWaiting++;
        }

        pthread_mutex_lock(&main_lock);
        while (!workerAvailable)
        {
          printf("All workers are busy, please wait.\n");
          pthread_cond_wait(&main_cond, &main_lock);
        }
        pthread_mutex_unlock(&main_lock);

        if (clientsWaiting > 0)
        {
          clientsWaiting--;
        }

        //printf("Client number %d\n", clientNumber);
        //printf("Connection ID %d %d\n", conn, connectionID[clientNumber]);

        printf("Got a connection.\n");

        pthread_mutex_lock(&thr_lock);

        clientNumber++;
        connectionID[clientNumber] = conn;

        pthread_cond_signal(&thr_cond);
        checkClient = true;
        //pass conn variable to thread

        pthread_mutex_unlock(&thr_lock);
        //checkClient = false;
        /*
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
          */
        }
      //}
    //}
    //else
    //{
    //  clientWaiting++;
    //}
  }

  for (int i=0; i<NTHREADS; i++)
  {
      pthread_join(worker_thread[i], NULL);   //check error!
  }

  return 0;
}
