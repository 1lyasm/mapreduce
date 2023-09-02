#ifndef ERR_H
#define ERR_H

#include <stdio.h>
#include <stdlib.h>

void err(char *msg) {
  fprintf(stderr, "%s\n", msg);
  exit(EXIT_FAILURE);
}

#endif
