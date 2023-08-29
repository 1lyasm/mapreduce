#include <locale.h>
#include <stdio.h>
#include <stdlib.h>

#include "../mr/mr.h"
#include "../util/log.h"

int main(int argc, char **argv) {
  setlocale(LC_ALL, "en_US.utf8");
  if (argc < 2) {
    fprintf(stderr, "Usage: mrsequential xxx.so inputfiles...\n");
    exit(EXIT_FAILURE);
  }
  Kva *intm = allocKva();
  for (int i = 1; i < argc; ++i) {
    char *fName = argv[i];
    log(printf("main: file number: %d, fName: %s\n", i - 1, fName));
    FILE *f = fopen(fName, "r");
    if (!f) {
      fprintf(stderr, "main: fopen failed\n");
      exit(EXIT_FAILURE);
    }
    if (fseek(f, 0, SEEK_END)) {
      fprintf(stderr, "main: fseek faile\n");
      exit(EXIT_FAILURE);
    }
    long fLen = ftell(f);
    if (fLen == -1L) {
      fprintf(stderr, "main: ftell failed\n");
      exit(EXIT_FAILURE);
    }
    log(printf("main: fLen: %ld\n", fLen));
    rewind(f);
    char *buf = malloc((fLen + 1) * sizeof(char));
    if (!buf) {
      fprintf(stderr, "main: malloc failed\n");
      exit(EXIT_FAILURE);
    }
    buf[fLen] = 0;
    log(printf(
        "main: fread args: buffer: %p, size: %zu, count: %zu, file: %p\n", buf,
        sizeof(char), fLen, f));
    size_t readCnt = fread(buf, sizeof(char), fLen, f);
    log(printf("main: readCnt: %zu\n", readCnt));
    if (readCnt != fLen) {
      if (feof(f)) {
        fprintf(stderr, "main: fread: unexpected EOF\n");
        exit(EXIT_FAILURE);
      } else if (ferror(f)) {
        fprintf(stderr, "main: fread: ferror returned true\n");
        exit(EXIT_FAILURE);
      } else {
        fprintf(stderr, "main: fread failed\n");
        exit(EXIT_FAILURE);
      }
    }
    if (fclose(f) == EOF) {
      fprintf(stderr, "main: fclose failed\n");
      exit(EXIT_FAILURE);
    }
    Kva *kva = map(fName, buf);
    for (int j = 0; j < kva->len; ++j) {
      addKv(intm, copyKv(kva->data[j]));
    }
    free(buf);
    freeKva(kva);
  }
  freeKva(intm);
  return 0;
}
