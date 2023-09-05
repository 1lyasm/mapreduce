#include "err.h"

#include <stdio.h>
#include <stdlib.h>

void err(char *msg) {
    fprintf(stderr, "%s\n", msg);
    exit(EXIT_FAILURE);
}
