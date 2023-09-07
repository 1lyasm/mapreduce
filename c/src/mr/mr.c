#include "mr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../util/err.h"

Kv *allocKv(int kLen, int vLen) {
    Kv *kv = malloc(sizeof(Kv));
    if (!kv) err("allocKv: malloc failed");
    kv->k = malloc((kLen + 1) * sizeof(char));
    if (!kv->k) err("allocKv: malloc failed");
    kv->k[kLen] = 0;
    kv->v = malloc((vLen + 1) * sizeof(char));
    if (!kv->v) err("allocKv: malloc failed");
    kv->v[vLen] = 0;
    return kv;
}

Kv *copyKv(Kv *kv) {
    Kv *copy = allocKv(strlen(kv->k), strlen(kv->v));
    memcpy(copy->k, kv->k, strlen(kv->k));
    memcpy(copy->v, kv->v, strlen(kv->v));
    return copy;
}

void printKv(Kv *kv) { printf("{ \"%s\", \"%s\" }\n", kv->k, kv->v); }

void freeKv(Kv *kv) {
    free(kv->k);
    free(kv->v);
    free(kv);
}

Kva *allocKva(void) {
    Kva *kva = malloc(sizeof(Kva));
    if (!kva) err("allocKva: malloc failed");
    kva->len = 0;
    kva->size = 1;
    kva->data = malloc(kva->size * sizeof(Kv *));
    if (!kva->data) err("allocKva: malloc failed");
    return kva;
}

void expandKva(Kva *kva) {
    kva->size *= 2;
    kva->data = realloc(kva->data, kva->size * sizeof(Kv *));
    if (!kva->data) err("expandKva: realloc failed");
}

void addKv(Kva *kva, Kv *kv) {
    if (kva->len == kva->size) {
        expandKva(kva);
    }
    kva->data[kva->len++] = kv;
}

void printKva(Kva *kva) {
    printf("{\n");
    for (int i = 0; i < kva->len; ++i) {
        printf("\t");
        printKv(kva->data[i]);
    }
    printf("}\n");
}

void freeKva(Kva *kva) {
    for (int i = 0; i < kva->len; ++i) freeKv(kva->data[i]);
    free(kva->data);
    free(kva);
}
