#ifndef COMMON_H
#define COMMON_H

#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

/* ---- parametry (latwa zmiana) ---- */
#define B 3      /* liczba busow (niezaleznych sekcji krytycznych) */
#define P 10     /* pojemnosc kazdego busa (jednostki towaru) */
#define X 4      /* maksymalny rozmiar zamowienia jednego mieszkanca */

#define MAX_PROC 128
#define MAX_Q    256     /* max wpisow w kolejce jednego busa */

#define DEBUG 1          /* 1 = szczegolowe logi, 0 = tylko stany */

/* typy wiadomosci = tagi MPI */
#define MSG_REQ     1
#define MSG_ACK     2
#define MSG_ARRIVED 3

/* stany procesu */
typedef enum { REST, WAIT, ONBUS, DRIVING } state_t;

/* wpis kolejki - jedno zadanie REQ */
typedef struct {
    int ts;
    int id;
    int order;
} req_t;

/* pakiet wymieniany przez MPI (wysylany jako MPI_BYTE) */
typedef struct {
    int ts;                 /* etykieta czasowa Lamporta */
    int bus;                /* numer busa (REQ, ARRIVED) */
    int order;              /* rozmiar zamowienia (REQ) */
    int driver;             /* id kierowcy (ARRIVED) */
    int npass;              /* liczba pasazerow (ARRIVED) */
    int pass[MAX_PROC];     /* id pasazerow (ARRIVED) */
} packet_t;

/* ---- globalne, chronione mutexem ---- */
extern int rank, size;
extern int clock_l;                 /* zegar Lamporta */
extern pthread_mutex_t lock;
extern pthread_cond_t  cv;          /* sygnal: wrocilem do REST */

extern state_t st;
extern req_t queue[B][MAX_Q];
extern int qlen[B];
extern int lastMsg[MAX_PROC];       /* najnowszy ts wiadomosci od j */

extern int myBus, myOrder, myTS;
extern packet_t arrivedBuf;
extern int hasArrivedBuf;

extern volatile int finished;

/* util.c */
int  tick(void);                    /* clock_l++ ; zwraca nowa wartosc */
void clock_update(int ts);          /* clock_l = max(clock_l, ts) + 1 */
void log_state(const char *msg);    /* [id] [tNNN] msg  (wolac pod lockiem) */
void log_dbg(const char *fmt, ...); /* logi szczegolowe (DEBUG) */

/* queue.c */
void q_insert(int b, int ts, int id, int order);
void q_remove_id(int b, int id);
int  q_find_pos(int b, int id);
int  load_before(int b, int pos);
int  proc_in_any_queue(int id);

#endif
