#!/usr/bin/bash
mkdir -p bin/
clang -O2 -Wall -Werror -Wpedantic -fsanitize=$2 \
    src/main/mrsequential.c src/mr/mr.c src/apps/$1.c -o bin/mrsequential
