#include "coordinator.h"

#include "../util/err.h"
#include "../util/log.h"

int done(Coord *c) {
    if (mtx_lock(&c->muRedCnt) == thrd_error) err("done: mtx_lock failed");
    int redCnt = c->redCnt;
    if (mtx_unlock(&c->muRedCnt) == thrd_error) err("done: mtx_unlock failed");
    return redCnt == c->nRed;
}

Tasks *initTasks(char **fNames, int fCnt, int nRed) {
	Tasks *tasks = malloc(fCnt * sizeof(Task));
	if (!tasks) {
		err("initTasks: malloc failed");
	}
    for (int i = 0; i < fCnt; ++i) {
        log(printf("initTasks: new task with file: %s\n", fNames[i]));

    }
    retunr tasks;
}

Coord *makeCoord(char **fNames, int fCnt, int nRed) {
    Coord *c = malloc(sizeof(Coord));
    if (!c) {
        err("makeCoord: malloc failed");
    }
    c->fCnt = fCnt;
    c->tasks = initTasks(fNames, fCnt, nRed);
    return NULL;
}
