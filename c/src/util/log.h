#ifndef LOG_H
#define LOG_H

#define LOG

#ifdef LOG
#define log(x)   \
    printTime(); \
    x
#else
#define log(x)
#endif

void printTime();

#endif
