#include "mr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

Kv *allocKv(int kLen, int vLen) {
  Kv *kv = malloc(sizeof(Kv));
  if (!kv) {
    fprintf(stderr, "allocKv: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  kv->k = malloc((kLen + 1) * sizeof(char));
  if (!kv->k) {
    fprintf(stderr, "allocKv: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  kv->k[kLen] = 0;
  kv->v = malloc((vLen + 1) * sizeof(char));
  if (!kv->v) {
    fprintf(stderr, "allocKv: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  kv->v[vLen] = 0;
  return kv;
}

Kv *copyKv(Kv *kv) {
  Kv *copy = allocKv(strlen(kv->k), strlen(kv->v));
  memcpy(copy->k, kv->k, strlen(kv->k));
  memcpy(copy->v, kv->v, strlen(kv->v));
  return copy;
}

void freeKv(Kv *kv) {
  free(kv->k);
  free(kv->v);
  free(kv);
}

Kva *allocKva(void) {
  Kva *kva = malloc(sizeof(Kva));
  if (!kva) {
    fprintf(stderr, "allocKva: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  kva->len = 0;
  kva->size = 1;
  kva->data = malloc(kva->size * sizeof(Kv *));
  if (!kva->data) {
    fprintf(stderr, "allocKva: malloc failed\n");
    exit(EXIT_FAILURE);
  }
  return kva;
}

void expandKva(Kva *kva) {
  kva->size *= 2;
  kva->data = realloc(kva->data, kva->size * sizeof(Kv *));
  if (!kva->data) {
    fprintf(stderr, "expandKva: realloc failed\n");
    exit(EXIT_FAILURE);
  }
}

void addKv(Kva *kva, Kv *kv) {
  if (kva->len == kva->size) {
    expandKva(kva);
  }
  kva->data[kva->len++] = kv;
}

void freeKva(Kva *kva) {
  for (int i = 0; i < kva->len; ++i) {
    freeKv(kva->data[i]);
  }
  free(kva->data);
  free(kva);
}
