#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../mr/mr.h"
#include "../util/err.h"
#include "../util/log.h"

Kva *map(char *fName, char *str);
char *reduce(char *key, char **vals, int valsLen);

int byKey(const void *a, const void *b) {
  char *arg1 = (*((Kv **)a))->k, *arg2 = (*((Kv **)b))->k;
  return strcmp(arg1, arg2);
}

int main(int argc, char **argv) {
  if (argc < 2) err("Usage: mrsequential inputfiles...");
  setlocale(LC_ALL, "en_US.utf8");
  Kva *intm = allocKva();
  for (int i = 1; i < argc; ++i) {
    char *fName = argv[i];
    log(printf("main: file number: %d, fName: %s\n", i - 1, fName));
    FILE *f = fopen(fName, "r");
    if (!f) err("main: fopen failed");
    if (fseek(f, 0, SEEK_END)) err("main: fseek failed");
    long fLen = ftell(f);
    if (fLen == -1L) err("main: ftell failed");
    log(printf("main: fLen: %ld\n", fLen));
    rewind(f);
    char *str = malloc((fLen + 1) * sizeof(char));
    if (!str) err("main: malloc failed");
    str[fLen] = 0;
    size_t readCnt = fread(str, sizeof(char), fLen, f);
    log(printf("main: readCnt: %zu\n", readCnt));
    if (readCnt != fLen) {
      if (feof(f))
        err("main: fread: unexpected EOF");
      else if (ferror(f))
        err("main: fread: ferror returned true");
      else
        err("main: fread failed");
    }
    if (fclose(f) == EOF) err("main: fclose failed");
    Kva *kva = map(fName, str);
    for (int j = 0; j < kva->len; ++j) addKv(intm, copyKv(kva->data[j]));
    free(str);
    freeKva(kva);
  }
  qsort(intm->data, intm->len, sizeof(Kv *), &byKey);
  FILE *outF = fopen("mr-out-0", "w");
  if (!outF) err("main: fopen failed");
  for (int i = 0; i < intm->len;) {
    int j = i + 1;
    for (; j < intm->len; ++j)
      if (strcmp(intm->data[j]->k, intm->data[i]->k) != 0) break;
    int valuesLen = 0, valuesSize = j - i;
    char **values = malloc(valuesSize * sizeof(char *));
    if (!values) err("main: malloc failed");
    for (int k = i; k < j; ++k) {
      char *val = intm->data[k]->v;
      int valLen = strlen(val);
      char *newVal = malloc((valLen + 1) * sizeof(char));
      if (!newVal) err("main: malloc failed");
      newVal[valLen] = 0;
      memcpy(newVal, val, valLen * sizeof(char));
      values[valuesLen++] = newVal;
    }
    char *output = reduce(intm->data[i]->k, values, valuesLen);
    fprintf(outF, "%s %s\n", intm->data[i]->k, output);
    for (int k = 0; k < valuesLen; ++k) free(values[k]);
    free(values);
    free(output);
    i = j;
  }
  if (fclose(outF) == EOF) err("main: fclose failed");
  freeKva(intm);
  return 0;
}
