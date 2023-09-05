#include <locale.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <wctype.h>

#include "../mr/mr.h"
#include "../util/err.h"
#include "../util/log.h"

char *findFirst(char *str, int isLetter) {
    size_t n = sizeof(int), readCnt;
    mbstate_t state;
    memset(&state, 0, sizeof(state));
    char *cur;
    for (cur = str; *cur != 0; cur += readCnt) {
        wchar_t wc;
        readCnt = mbrtowc(&wc, cur, n, &state);
        if (readCnt == 0 || readCnt == (size_t)-1 || readCnt == (size_t)-2)
            err("findFirst: mbrtowc failed");
        if ((isLetter && iswalpha(wc)) || (!isLetter && !iswalpha(wc)))
            return cur;
    }
    return cur;
}

void map(char *fName, char *str, Kva *kva) {
    char *left = findFirst(str, 1), *right = findFirst(left, 0);
    while (*right) {
        Kv *kv = allocKv(right - left, 1);
        memcpy(kv->k, left, (right - left) * sizeof(char));
        kv->v[0] = '1';
        addKv(kva, kv);
        left = findFirst(right, 1);
        right = findFirst(left, 0);
    }
}

char *reduce(char *key, char **vals, int valsLen) {
    int len = snprintf(NULL, 0, "%d", valsLen);
    if (len < 0) err("reduce: snprintf failed");
    char *str = malloc((len + 1) * sizeof(char));
    if (!str) err("reduce: malloc failed");
    snprintf(str, (len + 1) * sizeof(char), "%d", valsLen);
    return str;
}
