#include <locale.h>
#include <stdio.h>
#include <stdlib.h>

#include "../mr/mr.h"
#include "../util/err.h"
#include "../util/log.h"

Kva *map(char *fName, char *str);
char *reduce(char *key, char **vals, int valsLen);

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
    freeKva(kva);
  }
  freeKva(intm);
  return 0;
}
