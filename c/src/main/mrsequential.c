#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../mr/mr.h"
#include "../util/err.h"
#include "../util/log.h"

int byKey(const void *a, const void *b) {
    char *arg1 = (*(Kv **)a)->k, *arg2 = (*(Kv **)b)->k;
    return strcmp(arg1, arg2);
}

int main(int argc, char **argv) {
    if (argc < 2) err("Usage: mrsequential inputfiles...");
    int fCnt = argc - 1;
    long *fLens = malloc(fCnt * sizeof(long));
    if (!fLens) err("main: malloc failed");
    for (int i = 1; i < argc; ++i) {
        FILE *f = fopen(argv[i], "r");
        if (!f) err("main: fopen failed");
        if (fseek(f, 0, SEEK_END)) err("main: fseek failed");
        long fLen = ftell(f);
        if (fLen == -1L) err("main: ftell failed");
        fLens[i - 1] = fLen;
        if (fclose(f) == EOF) err("main: fclose failed");
    }
    long maxFLen = -1;
    for (int i = 0; i < fCnt; ++i)
        maxFLen = fLens[i] > maxFLen ? fLens[i] : maxFLen;
    Kva *kva = allocKva();
    char *str = malloc((maxFLen + 1) * sizeof(char));
    if (!str) err("main: malloc failed");
    for (int i = 1; i < argc; ++i) {
        char *fName = argv[i];
        log(printf("main: file number: %d, fName: %s\n", i - 1, fName));
        FILE *f = fopen(fName, "r");
        if (!f) err("main: fopen failed");
        long fLen = fLens[i - 1];
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
        map(fName, str, kva);
    }
    free(fLens);
    free(str);
    qsort(kva->data, kva->len, sizeof(Kv *), &byKey);
    FILE *outF = fopen("mr-out-0", "w");
    if (!outF) err("main: fopen failed");
    char **values = malloc(kva->len * sizeof(char *));
    if (!values) err("main: malloc failed");
    for (int i = 0; i < kva->len;) {
        int j = i + 1;
        for (; j < kva->len; ++j)
            if (strcmp(kva->data[j]->k, kva->data[i]->k) != 0) break;
        int valuesLen = 0;
        for (int k = i; k < j; ++k) values[valuesLen++] = kva->data[k]->v;
        char *output = reduce(kva->data[i]->k, values, valuesLen);
        fprintf(outF, "%s %s\n", kva->data[i]->k, output);
        free(output);
        i = j;
    }
    if (fclose(outF) == EOF) err("main: fclose failed");
    free(values);
    freeKva(kva);
    return 0;
}
