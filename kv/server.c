/* Server program for key-value store. */

#include "kv.h"
#include "parser.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#define NTHREADS = 4;
#define BACKLOG = 10;

/* Add anything you want here. */
pthread_mutex_t lock;
int workerID[100];

/* A worker thread. You should write the code of this function. */
void* worker(void* p) {

}

void run()
{
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

int socket()
