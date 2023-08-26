#!/usr/bin/bash

RACE=-race

(
    cd src/mrapps
    for FILE in $(ls *.go)
    do
        go build $RACE -buildmode=plugin $FILE || exit 1
    done
)

(
    cd src/main
    for FILE in $(ls *.go)
    do
        go build $RACE $FILE || exit 1
    done
)
