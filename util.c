#include "common.h"
#include <stdarg.h>

int tick(void) {
    clock_l++;
    return clock_l;
}

void clock_update(int ts) {
    if (ts > clock_l) clock_l = ts;
    clock_l++;
}

void log_state(const char *msg) {
    printf("[%d] [t%d] %s\n", rank, clock_l, msg);
    fflush(stdout);
}

void log_dbg(const char *fmt, ...) {
#if DEBUG
    va_list ap;
    va_start(ap, fmt);
    printf("[%d] [t%d] ", rank, clock_l);
    vprintf(fmt, ap);
    printf("\n");
    fflush(stdout);
    va_end(ap);
#else
    (void)fmt;
#endif
}
