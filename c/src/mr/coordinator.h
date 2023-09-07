#ifndef COORDINATOR_H
#define COORDINATOR_H

#include <threads.h>
#include <time.h>

typedef struct {
    int id;
    time_t last;
} Worker;

typedef struct {
    mtx_t mu;
    Worker *data;
} Workers;

typedef enum { Free, Live } TaskStat;

typedef enum { Map, Reduce } TaskType;

typedef struct {
    char *fName;
    TaskStat stat;
    TaskType type;
    int num;
    time_t start;
} Task;

typedef struct {
    mtx_t mu;
    Task *data;
} Tasks;

typedef struct {
    Workers *workers;
    Tasks *tasks;
    int nRed;
    mtx_t muRedCnt;
    int redCnt;
    int fCnt;
} Coord;

int done(Coord *c);

Coord *makeCoord(char **fNames, int fCnt, int nRed);

#endif
