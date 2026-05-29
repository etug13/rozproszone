#include "common.h"

static int higher(req_t a, req_t b) {
    if (a.ts != b.ts) return a.ts < b.ts;
    return a.id < b.id;
}

void q_insert(int b, int ts, int id, int order) {
    req_t e = { ts, id, order };
    int i = qlen[b];
    while (i > 0 && higher(e, queue[b][i - 1])) {
        queue[b][i] = queue[b][i - 1];
        i--;
    }
    queue[b][i] = e;
    qlen[b]++;
}

void q_remove_id(int b, int id) {
    int i, k = 0;
    for (i = 0; i < qlen[b]; i++)
        if (queue[b][i].id != id)
            queue[b][k++] = queue[b][i];
    qlen[b] = k;
}

int q_find_pos(int b, int id) {
    int i;
    for (i = 0; i < qlen[b]; i++)
        if (queue[b][i].id == id) return i;
    return -1;
}

int load_before(int b, int pos) {
    int i, s = 0;
    for (i = 0; i < pos; i++)
        s += queue[b][i].order;
    return s;
}

int proc_in_any_queue(int id) {
    int b;
    for (b = 0; b < B; b++)
        if (q_find_pos(b, id) >= 0) return 1;
    return 0;
}
