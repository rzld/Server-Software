/* Server program for key-value store. */

//References:
// https://gist.github.com/batuhangoksu/2b3afe5970b262d54626
// http://stackoverflow.com/questions/9488185/waking-up-individual-threads-instead-of-busy-wait-in-pthreads

#include "kv.h"
#include "parser.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/poll.h>
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
bool checkClient, workerAvailable, work, ctrlPort;
int workerBusy, clientWaiting, nConnections;
//bool workerTaken[NTHREADS];
int run, connectionID[100];
int totalWorker = NTHREADS;
int clientNumber, clientsWaiting;

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {
  int workerID_ = *(int*) p;
  int socket_;
  int inputSize;
  char *message, *message2,
       buffer[255];

  work = true;

  while(work)
  {
    printf("Worker %d is waiting for connection.\n", workerID_);
    checkClient = false;
    //lock thread
    // workerTaken[socket_] = false;
    int err16 = pthread_mutex_lock(&main_lock);
    if (err16)
    {
      printf("Error: mutex locking failed!\n");
      exit(1);
    }

    if (clientsWaiting > 0)
    {
      int err17 = pthread_cond_signal(&main_cond);
      if (err17)
      {
        printf("Error: condition signal failed!\n");
        exit(1);
      }
      workerAvailable = true;
    }
    int err18 = pthread_mutex_unlock(&main_lock);
    if (err18)
    {
      printf("Error: mutex unlocking failed!\n");
      exit(1);
    }

    int err19 = pthread_mutex_lock(&thr_lock);
    if (err19)
    {
      printf("Error: mutex locking failed!\n");
      exit(1);
    }
    //wait for connection
    while(!checkClient)
    {
      int err20 = pthread_cond_wait(&thr_cond, &thr_lock);
      if (err20)
      {
        printf("Error: condition waiting failed!\n");
        exit(1);
      }
    }

    socket_ = connectionID[clientNumber];

    int err21 = pthread_mutex_unlock(&thr_lock);
    if (err21)
    {
      printf("Error: mutex unlocking failed!\n");
      exit(1);
    }
    //workerTaken[socket_] = true;

    if (ctrlPort)
    {
      close(socket_);
      break;
    }

    totalWorker--;
    printf("Worker %d executing task.\n", workerID_);
    //printf(">> Current available worker: %d\n\n", totalWorker);

    //welcome message
    message = "Welcome to the KV store.\n";
    write(socket_, message, strlen(message));
    message = "What do you want to do?\n";
    write(socket_, message, strlen(message));

    //read from client
    enum DATA_CMD cmd;    //data
    char* key;
    char* text;

    while (inputSize = read(socket_, buffer, sizeof(buffer)))
    {
      // end of string marker
      buffer[inputSize] = '\0';
      printf("%s\n", buffer);

      //parse the buffer
      parse_d(buffer, &cmd, &key, &text);

      // clear message buffer
      memset(buffer, 0, 255);

      //write response line
      if (cmd == D_PUT)
      {
        //the line contains "put key value" command
        // the pointers key and text point to 0-terminated strings
        // containing the key and value
        char* copy = malloc(strlen(text) + 1);
        strncpy(copy, text, strlen(text) + 1);
        int put_ = createItem(key, copy);

        if (put_ < 0)
        {
          if (itemExists(key) != 0)
          {
            int update_ = updateItem(key, copy);
            message = "Updated.\n";
            write(socket_, message, strlen(message));
          }
          else
          {
            message = "Create item failed.\n";
            write(socket_, message, strlen(message));
          }
        }
        else
        {
          message = "Create item success.\n";
          write(socket_, message, strlen(message));
        }

        free(copy);
      }
      else if (cmd == D_GET)
      {
        //ntains a "get key" command
        // pointer key points to the key and text is null
        message2 = findValue(key);
        if (message2 == NULL)
        {
          strcpy(buffer, "Does not exist.\n");
        }
        else
        {
          snprintf(buffer, strlen(message2), "%s\n", message2);
         //memset(buffer, '\0', sizeof(buffer));
          //strcpy(buffer, message);
          //sprintf(buffer, "%s", message);
        }
        write(socket_, buffer, strlen(buffer));
      }
      else if (cmd == D_COUNT)
      {
        //contains "count" command. key and value null
        //message = "count\n";
        int itemsCount = countItems();
        sprintf(buffer, "%d\n", itemsCount);
        write(socket_, buffer, strlen(buffer));
        //sprintf(message, "%d", itemsCount);
        //write(socket_, message, strlen(message));
      }
      else if (cmd == D_DELETE)
      {
        //contains "delete" key. pointer key points to the key and text is null
        int del_ = deleteItem(key, 0);
        if (del_ < 0)
        {
          message = "Delete item Failed.\n";
          write(socket_, message, strlen(message));
        }
        else
        {
          message = "Delete item Success.\n";
          write(socket_, message, strlen(message));
        }
      }
      else if (cmd == D_EXISTS)
      {
        //contains "exists" key command. pointer key points to the key and text is null
        int exists_ = itemExists(key);
        sprintf(buffer, "%d\n", exists_);
        write(socket_, buffer, strlen(buffer));
      }
      else if (cmd == D_END)
      {
        printf("Disconnected.\n");
        //line empty, close connection
        totalWorker++;
        fflush(stdout);
        int errc3 = close(socket_);
        if (errc3 < 0)
        {
          printf("Closing failed.\n");
          return -1;
        }
        break;
      }
      else if (cmd == D_ERR_OL)
      {
        //error: line too long
        message = "Line too long.\n";
        write(socket_, message, strlen(message));
      }
      else if (cmd == D_ERR_INVALID)
      {
        //error: invalid command
        message = "Invalid command.\n";
        write(socket_, message, strlen(message));
      }
      else if (cmd == D_ERR_SHORT)
      {
        //error: too few parameters
        //still not working
        message = "Too few parameters.\n";
        write(socket_, message, strlen(message));
      }
      else if (cmd == D_ERR_LONG)
      {
        //error: too many parameters
        //still not working
        message = "Too many parameters.\n";
        write(socket_, message, strlen(message));
      }

      // clear message buffer
      memset(buffer, 0, 255);

    }
  }
  pthread_exit(NULL);
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
  int controlSocket, dataSocket, workerID[NTHREADS];
  struct sockaddr_in controls, clients;
  pthread_t worker_thread[NTHREADS];
  struct pollfd ufds[2];

  int err1 = pthread_mutex_init(&main_lock, NULL);  //dont forget error handling!
  int err2 = pthread_mutex_init(&thr_lock, NULL);
  int err3 = pthread_cond_init(&main_cond, NULL);
  int err4 = pthread_cond_init(&thr_cond, NULL);

  if (err1 || err2)
  {
    printf("Error: mutex initialization failed!\n");
    exit(1);
  }
  if (err3 || err4)
  {
    printf("Error: mutex condition initialization failed!\n");
    exit(1);
  }

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
    int err5 = pthread_create(&worker_thread[i], NULL, worker, (void*)&workerID[i]);
    if (err5)
    {
      printf("Error: thread creation failed!\n");
      exit(1);
    }
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
  int errL1 = listen(controlSocket, BACKLOG);
  int errL2 = listen(dataSocket, BACKLOG);
  if (errL1 < 0 || errL2 < 0)
  {
    printf("Listen error.\n");
    return 1;
  }

  //waiting for incoming connection
  printf("Waiting for clients...\n");
  workerAvailable = true;

  int c = sizeof(struct sockaddr_in);

  //accept any incoming connection
  while (run)
  {
    //if (workerBusy < NTHREADS)
    //{
    ufds[0].fd = controlSocket;
    ufds[0].events = POLLIN;

    ufds[1].fd = dataSocket;
    ufds[1].events = POLLIN;

    int rv = poll(ufds, 2, 5000);

    if (rv == -1)
    {
      printf("Error occured in poll.\n");
    }
    /*else if (rv == 0)
    {
      printf("Timeout!\n");
    }*/
    else
    {
      //data socket
      if (ufds[1].revents & POLLIN)
      {
        int conn = accept(dataSocket, (struct sockaddr *)&clients, (socklen_t*)&c);

        if (conn == -1)
        {
          printf("Connection error!\n");
        }
        else
        {
          //printf("> Available worker: %d.\n", totalWorker);

          if (totalWorker == 0)
          {
            workerAvailable = false;
            clientsWaiting++;
          }

          int err6 = pthread_mutex_lock(&main_lock);
          if (err6)
          {
            printf("Error: mutex lock failed.\n");
            exit(1);
          }
          while (!workerAvailable)
          {
            printf("All workers are busy, please wait.\n");
            int err7 = pthread_cond_wait(&main_cond, &main_lock);
            if (err7)
            {
              printf("Error: condition waiting failed.\n");
              exit(1);
            }
          }
          int err8 = pthread_mutex_unlock(&main_lock);
          if (err8)
          {
            printf("Error: mutex unlock failed.\n");
            exit(1);
          }

          if (clientsWaiting > 0)
          {
            clientsWaiting--;
          }

          //printf("Client number %d\n", clientNumber);
          //printf("Connection ID %d %d\n", conn, connectionID[clientNumber]);

          printf("Got a connection.\n");

          int err9 = pthread_mutex_lock(&thr_lock);
          if (err9)
          {
            printf("Error: mutex lock failed.\n");
            exit(1);
          }

          clientNumber++;
          connectionID[clientNumber] = conn;

          int err10 = pthread_cond_signal(&thr_cond);
          if (err10)
          {
            printf("Error: condition signal failed.\n");
            exit(1);
          }
          checkClient = true;
          //pass conn variable to thread

          int err11 = pthread_mutex_unlock(&thr_lock);
          if (err11)
          {
            printf("Error: mutex unlock failed.\n");
            exit(1);
          }
        }
      }
      //control socket
      if (ufds[0].revents & POLLIN)
      {
        int conn = accept(controlSocket, (struct sockaddr *)&controls, (socklen_t*)&c);
        int socket_;

        if (conn == -1)
        {
          printf("Connection error!\n");
        }
        else
        {
          socket_ = conn;
          printf("Connected to control port\n");
          ctrlPort = true;

          char* message, buffer[255];
          enum CONTROL_CMD cmd;    //data
          int inputSize;

          message = "> You are connected to the control port\n";
          write(socket_, message, strlen(message));
          message = "> What do you want to do?\n";
          write(socket_, message, strlen(message));

          while (inputSize = read(socket_, buffer, sizeof(buffer)))
          {
            // end of string marker
            buffer[inputSize] = '\0';

            //parse the buffer
            cmd = parse_c(buffer);

            // clear message buffer
            memset(buffer, 0, 255);

            if (cmd == C_SHUTDOWN)
            {
              message = "Shutting down.\n";
              write(socket_, message, strlen(message));

              if (totalWorker > 0)
              {
                int err12 = pthread_mutex_lock(&thr_lock);
                if (err12)
                {
                  printf("Error: mutex lock failed.\n");
                  exit(1);
                }

                int err13 = pthread_cond_broadcast(&thr_cond);
                if (err13)
                {
                  printf("Error: condition broadcast failed.\n");
                  exit(1);
                }

                checkClient = true;
                work = false;

                int err14 = pthread_mutex_unlock(&thr_lock);
                if (err14)
                {
                  printf("Error: mutex unlock failed.\n");
                  exit(1);
                }
              }

              message = "Waiting for other threads to finish...\n";
              write(socket_, message, strlen(message));

              //work = false;
              for (int i=0; i<NTHREADS; i++)
              {
                  int err15 = pthread_join(worker_thread[i], NULL);
                  if (err15)
                  {
                    printf("Error: join thread failed.\n");
                    exit(1);
                  }
              }

              close(socket_);
              run = false;
              break;
            }
            else if (cmd == C_COUNT)
            {
              int itemsCount = countItems();
              sprintf(buffer, "%d\n", itemsCount);
              write(socket_, buffer, strlen(buffer));
            }
            else if (cmd == C_ERROR)
            {
              message = "Command error.\n";
              write(socket_, message, strlen(message));
            }
          }
        }
      }
    }
  }

  int errs1 = shutdown(controlSocket, SHUT_RDWR);
  int errs2 = shutdown(dataSocket, SHUT_RDWR);
  int errc1 = close(controlSocket);
  int errc2 = close(dataSocket);

  if (errs1 < 0 || errs2 < 0)
  {
    printf("Shutdown failed.\n");
    return -1;
  }
  if (errc1 < 0 || errc2 < 0)
  {
    printf("Closing failed.\n");
    return -1;
  }
  printf("Session disconnected.\n");

  /*for (int i=0; i<NTHREADS; i++)
  {
      pthread_join(worker_thread[i], NULL);   //check error!
  }*/

  return 0;
}
