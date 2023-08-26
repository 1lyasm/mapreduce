#!/usr/bin/bash

rm -rf mr-tmp

(cd src/mrapps
go clean)

(cd src/main
go clean)
