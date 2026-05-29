#include "common.h"

int rank, size;
int clock_l = 0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  cv   = PTHREAD_COND_INITIALIZER;

state_t st = REST;
req_t queue[B][MAX_Q];
int qlen[B];
int lastMsg[MAX_PROC];

int myBus, myOrder, myTS;
packet_t arrivedBuf;
int hasArrivedBuf = 0;

volatile int finished = 0;

/* prototypy */
static void try_enter(void);
static void depart(int k);
static int  can_depart(int *k_out);
static int  in_pass_id(int id, int k);


static void send_ack(int dest) {
    packet_t a;
    memset(&a, 0, sizeof(a));
    a.ts = clock_l;
    MPI_Send(&a, sizeof(a), MPI_BYTE, dest, MSG_ACK, MPI_COMM_WORLD);
}

static void broadcast_req(void) {
    packet_t p;
    int j;
    memset(&p, 0, sizeof(p));
    p.bus = myBus;
    p.order = myOrder;
    p.ts = clock_l;
    for (j = 0; j < size; j++)
        if (j != rank)
            MPI_Send(&p, sizeof(p), MPI_BYTE, j, MSG_REQ, MPI_COMM_WORLD);
}

static void broadcast_arrived(packet_t *p) {
    int j;
    p->ts = clock_l;
    for (j = 0; j < size; j++)
        if (j != rank)
            MPI_Send(p, sizeof(*p), MPI_BYTE, j, MSG_ARRIVED, MPI_COMM_WORLD);
}


/* liczba REQ ktore mieszcza sie w busie b (prefiks kolejki); zwraca tez sume */
static int count_fitted(int b, int *load_out) {
    int i, used = 0, k = 0;
    for (i = 0; i < qlen[b]; i++) {
        if (used + queue[b][i].order <= P) {
            used += queue[b][i].order;
            k++;
        } else {
            break;          /* pierwszy ktory sie nie miesci = P1 */
        }
    }
    if (load_out) *load_out = used;
    return k;
}

/* W1: load_before(i) + myOrder <= P */
static int check_W1(void) {
    int pos = q_find_pos(myBus, rank);
    if (pos < 0) return 0;
    return load_before(myBus, pos) + myOrder <= P;
}

/* W2: dla kazdego j != i lastMsg[j] > myTS */
static int check_W2(void) {
    int j;
    for (j = 0; j < size; j++)
        if (j != rank && lastMsg[j] <= myTS) return 0;
    return 1;
}

/* ---- obsluga ARRIVED ---- */

static int in_pass(packet_t *p, int id) {
    int i;
    for (i = 0; i < p->npass; i++)
        if (p->pass[i] == id) return 1;
    return 0;
}

static void remove_passengers(int b, packet_t *p, int keep_self) {
    int i;
    for (i = 0; i < p->npass; i++) {
        if (keep_self && p->pass[i] == rank) continue;
        q_remove_id(b, p->pass[i]);
    }
}

/* wysiadam z busa -> REST */
static void leave_to_rest(int b, packet_t *p) {
    remove_passengers(b, p, 0);
    hasArrivedBuf = 0;
    st = REST;
    log_state("Wysiadam z busa (wychodze z sekcji krytycznej)");
}

static void on_arrived(packet_t *p) {
    int b = p->bus;
    int me = in_pass(p, rank);

    if (b == myBus && me && st == ONBUS) {
        leave_to_rest(b, p);
        return;
    }
    if (b == myBus && me && st == WAIT) {
        /* kierowca wrocil zanim spelnilem W2 - buforuj, swoj REQ zachowaj
           by W1+W2 moglo jeszcze potwierdzic wejscie */
        arrivedBuf = *p;
        hasArrivedBuf = 1;
        remove_passengers(b, p, 1);
        return;
    }
    /* pozostale przypadki: po prostu usun pasazerow z queue[b] */
    remove_passengers(b, p, 0);
}

/* ---- przejscia stanow ---- */

/* czy proces j jest pasazerem (jednym z pierwszych k w queue[myBus]) */
static int in_pass_id(int id, int k) {
    int i;
    for (i = 0; i < k; i++)
        if (queue[myBus][i].id == id) return 1;
    return 0;
}

/* czy lista pasazerow myBus jest ostateczna (warunek odjazdu).
   Ustawia *k_out na liczbe pasazerow. */
static int can_depart(int *k_out) {
    int load_fitted, k, j;
    k = count_fitted(myBus, &load_fitted);
    if (k_out) *k_out = k;

    if (k < qlen[myBus]) {
        /* warunek (1): P1 (pierwszy nie mieszczacy sie) blokuje kolejke oraz
           zaden nie-pasazer nie ma w locie REQ z ts < ts(P1) */
        req_t P1 = queue[myBus][k];
        for (j = 0; j < size; j++) {
            if (j == rank) continue;
            if (in_pass_id(j, k)) continue;
            if (lastMsg[j] < P1.ts) return 0;
        }
        return 1;
    }
    /* warunek (2): brak REQ za ostatnim pasazerem i nikt nie jest w REST */
    for (j = 0; j < size; j++) {
        if (j == rank) continue;
        if (!proc_in_any_queue(j)) return 0;
    }
    return 1;
}

/* kierowca: jedzie po towar i rozglasza ARRIVED */
static void depart(int k) {
    packet_t p;
    int i;
    memset(&p, 0, sizeof(p));
    p.bus = myBus;
    p.driver = rank;
    p.npass = k;
    for (i = 0; i < k; i++)
        p.pass[i] = queue[myBus][i].id;

    log_state("Bus pelny/popyt wyczerpany - jade po towar");
    /* przejazd - operacja lokalna */

    tick();
    broadcast_arrived(&p);
    remove_passengers(myBus, &p, 0);
    st = REST;
    log_state("Wrocilem i rozdalem towar (wychodze z sekcji krytycznej)");
}

/* WAIT: po spelnieniu W1+W2 wejdz na bus jako pasazer lub - gdy lista
   pasazerow jest juz ostateczna - jako kierowca. */
static void try_enter(void) {
    int k, pos;
    if (st != WAIT) return;
    if (!(check_W1() && check_W2())) return;

    k = count_fitted(myBus, NULL);
    pos = q_find_pos(myBus, rank);   /* dzieki W1: pos < k */

    if (pos < k - 1) {
        /* za mna jest mieszczacy sie pasazer => nigdy nie bede kierowca
           (kolejka tylko rosnie). Bezpiecznie wsiadam jako pasazer. */
        st = ONBUS;
        log_state("Jestem na busie (w sekcji krytycznej) - czekam na kierowce");
        if (hasArrivedBuf)
            leave_to_rest(arrivedBuf.bus, &arrivedBuf);
        return;
    }

    /* jestem aktualnie ostatnim pasazerem - kandydat na kierowce.
       Role kierowcy commituje dopiero gdy lista pasazerow jest ostateczna,
       inaczej ktos moze jeszcze dosiasc sie za mna. */
    if (can_depart(&k)) {
        st = DRIVING;
        log_state("Jestem kierowca - lista pasazerow ostateczna");
        depart(k);
    }
}

/* ---- obsluga pojedynczej wiadomosci (pod lockiem) ---- */

static void on_message(int src, int tag, packet_t *p) {
    clock_update(p->ts);
    lastMsg[src] = p->ts;

    switch (tag) {
    case MSG_REQ:
        q_insert(p->bus, p->ts, src, p->order);
        tick();
        send_ack(src);
        log_dbg("REQ od %d do busa %d (order=%d), odeslalem ACK", src, p->bus, p->order);
        break;
    case MSG_ACK:
        /* ignoruj - sluzy tylko do aktualizacji lastMsg (W2) */
        break;
    case MSG_ARRIVED:
        log_dbg("ARRIVED od kierowcy %d, bus %d", p->driver, p->bus);
        on_arrived(p);
        break;
    }

    if (st == WAIT) try_enter();
}

/* ---- watek komunikacyjny ---- */

static void *comm_loop(void *arg) {
    MPI_Status status;
    packet_t p;
    (void)arg;
    while (!finished) {
        MPI_Recv(&p, sizeof(p), MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG,
                 MPI_COMM_WORLD, &status);
        pthread_mutex_lock(&lock);
        on_message(status.MPI_SOURCE, status.MPI_TAG, &p);
        if (st == REST) pthread_cond_signal(&cv);
        pthread_mutex_unlock(&lock);
    }
    return NULL;
}

/* ---- watek glowny (logika mieszkanca) ---- */

static void wait_until_rest(void) {
    pthread_mutex_lock(&lock);
    while (st != REST)
        pthread_cond_wait(&cv, &lock);
    pthread_mutex_unlock(&lock);
}

static void *main_loop(void *arg) {
    (void)arg;
    while (!finished) {
        pthread_mutex_lock(&lock);
        st = REST;
        log_state("Spie");
        pthread_mutex_unlock(&lock);

        usleep((rand() % 1000 + 200) * 1000);   /* losowy sen */

        pthread_mutex_lock(&lock);
        myBus = rand() % B;
        myOrder = rand() % X + 1;
        hasArrivedBuf = 0;
        tick();
        myTS = clock_l;
        q_insert(myBus, myTS, rank, myOrder);
        st = WAIT;
        log_state("Rozpoczynam staranie o miejsce w busie");
        log_dbg("Wybralem bus %d, zamowienie %d", myBus, myOrder);
        broadcast_req();
        try_enter();
        pthread_mutex_unlock(&lock);

        wait_until_rest();
    }
    return NULL;
}

int main(int argc, char **argv) {
    int prov;
    pthread_t comm;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &prov);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (prov < MPI_THREAD_MULTIPLE && rank == 0)
        fprintf(stderr, "Uwaga: brak MPI_THREAD_MULTIPLE\n");

    srand(time(NULL) + rank * 100);
    memset(qlen, 0, sizeof(qlen));
    memset(lastMsg, 0, sizeof(lastMsg));

    pthread_create(&comm, NULL, comm_loop, NULL);
    main_loop(NULL);            /* wieczna petla */
    pthread_join(comm, NULL);

    MPI_Finalize();
    return 0;
}
