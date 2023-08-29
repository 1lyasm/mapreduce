#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>

#include "../mr/mr.h"
#include "../util/log.h"

char *findFirst(char *str, int isLetter) {
  size_t n = strlen(str), readCnt;
  mbstate_t state;
  memset(&state, 0, sizeof(state));
  char *cur;
  for (cur = str; *cur != 0; cur += readCnt) {
    wchar_t wc;
    readCnt = mbrtowc(&wc, cur, n, &state);
    if (readCnt == 0 || readCnt == (size_t)-1 || readCnt == (size_t)-2) {
      fprintf(stderr, "findFirst: mbrtowc failed\n");
      exit(EXIT_FAILURE);
    }
    if ((isLetter && iswalnum(wc)) || (!isLetter && !iswalnum(wc))) return cur;
  }
  return cur;
}

Kva *map(char *fName, char *str) {
  Kva *kva = allocKva();
  char *left = NULL, *right = NULL;
  left = findFirst(str, 1);
  right = findFirst(left, 0);
  while (*right) {
    Kv *kv = allocKv(right - left, 1);
    memcpy(kv->k, left, (right - left) * sizeof(char));
    kv->v[0] = '1';
    addKv(kva, kv);
    left = findFirst(right, 1);
    right = findFirst(left, 0);
  }
  return kva;
}

char *reduce(char *key, char **vals, int valsLen) {
  int len = snprintf(NULL, 0, "%d", valsLen);
  if (len < 0) {
    fprintf(stderr, "reduce: snprintf failed\n");
    exit(EXIT_FAILURE);
  }
  char *str = malloc((len + 1) * sizeof(char));
  if (!str) {
    fprintf(stderr, "reduce: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  snprintf(str, (len + 1) * sizeof(char), "%d", valsLen);
  return str;
}
