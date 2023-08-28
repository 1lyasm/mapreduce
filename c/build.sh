#!/usr/bin/bash
clang -g -O2 -Wall -Werror -std=c11 \
    -fsanitize=address -Wno-declaration-after-statement \
    src/main/mrsequential.c src/mr/worker.c -o src/main/mrsequential
