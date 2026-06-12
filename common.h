#ifndef COMMON_H
#define COMMON_H

#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define B 3
#define P 10
#define X 4

#define MAX_PROC 128
#define MAX_Q    256

#define DEBUG 1
#define LOG_DELAY_MS 300

#define MSG_REQ     1
#define MSG_ACK     2
#define MSG_ARRIVED 3

typedef enum { REST, WAIT, ONBUS, DRIVING } state_t;

typedef struct {
    int ts;
    int id;
    int order;
} req_t;

typedef struct {
    int ts;
    int bus;
    int order;
    int driver;
    int npass;
    int pass[MAX_PROC];
} packet_t;

extern int rank, size;
extern int clock_l;
extern pthread_mutex_t lock;
extern pthread_cond_t  cv;

extern state_t st;
extern req_t queue[B][MAX_Q];
extern int qlen[B];
extern int lastMsg[MAX_PROC];

extern int myBus, myOrder, myTS;
extern packet_t arrivedBuf;
extern int hasArrivedBuf;

extern volatile int finished;

int  tick(void);
void clock_update(int ts);
void log_state(const char *msg);
void log_dbg(const char *fmt, ...);

void q_insert(int b, int ts, int id, int order);
void q_remove_id(int b, int id);
int  q_find_pos(int b, int id);
int  load_before(int b, int pos);
int  proc_in_any_queue(int id);

#endif
