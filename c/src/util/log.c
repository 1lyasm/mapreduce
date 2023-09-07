#include "log.h"

#include <stdio.h>
#include <time.h>

#include "err.h"

#define BUF_SIZE 64

void printTime() {
    time_t now = time(NULL);
    if (now == (time_t)-1) err("printTime: time failed");
    struct tm *timeInfo = localtime(&now);
    if (!timeInfo) err("printTime: time failed");
    printf("%02d:%02d:%02d ", timeInfo->tm_hour, timeInfo->tm_min,
           timeInfo->tm_sec);
}
