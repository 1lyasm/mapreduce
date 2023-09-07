#include <stdio.h>
#include <threads.h>
#include <time.h>

#include "../mr/coordinator.h"
#include "../util/err.h"

int main(int argc, char **argv) {
    if (argc < 2) err("Usage: mrcoordinator inputfiles...");
    Coord *c = makeCoord(&argv[1], argc - 1, 10);
    while (!done(c)) {
        if (thrd_sleep(&(struct timespec){.tv_sec = 1}, NULL))
            err("main (mrcoordinator): thrd_sleep failed");
    }
    if (thrd_sleep(&(struct timespec){.tv_sec = 1}, NULL))
        err("main (mrcoordinator): thrd_sleep failed");
}
