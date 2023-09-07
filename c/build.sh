#!/usr/bin/bash
mkdir -p bin/
gcc -O2 -Wall -Werror -Wpedantic -fsanitize=$2 \
    src/main/mrsequential.c src/mr/*.c src/util/*.c src/apps/$1.c -o bin/mrsequential
gcc -O2 -Wall -Werror -Wpedantic -fsanitize=$2 \
    src/main/mrcoordinator.c src/mr/*.c src/util/*.c -o bin/mrcoordinator

