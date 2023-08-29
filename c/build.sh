#!/usr/bin/bash
clang -g -O0 -Wall -Werror -std=c11 \
    -fsanitize=address -Wno-declaration-after-statement \
    src/main/mrsequential.c src/mr/worker.c src/apps/wc.c -o mrsequential
