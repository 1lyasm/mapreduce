#include <stdio.h>
#include <stdlib.h>

#include "../mr/worker.h"
#include "../util/log.h"

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: mrsequential xxx.so inputfiles...\n");
        exit(EXIT_FAILURE);
    }
    Kva *intm = allocKva();
    char *buf = malloc(sizeof(char));
    for (int i = 2; i < argc; ++i) {
	char *fName = argv[i];
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
	buf = realloc(buf, (fLen + 1) * sizeof(char));
	if (!buf) {
	    fprintf(stderr, "main: realloc failed\n");
	    exit(EXIT_FAILURE);
	}
	buf[fLen] = 0;
	size_t readCnt = fread(buf, sizeof(char), fLen, f);
	if (readCnt != fLen) {
	    fprintf(stderr, "main: fread failed\n");
	    exit(EXIT_FAILURE);
	}
	log(printf("main: buf: %s\n", buf));
	if (fclose(f) == EOF) {
	    fprintf(stderr, "main: fclose failed\n");
	    exit(EXIT_FAILURE);
	}
	Kva *kva = mapf(fName, buf);
	for (int j = 0; j < kva->len; ++j) {
	    addKv(intm, copyKv(kva->data[j]));
	}
	freeKva(kva);
    }
    free(buf);
    freeKva(intm);
    return 0;
}

