#include <stdio.h>
#include <stdlib.h>

#include "err.h"

void err(char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}

