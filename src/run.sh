#!/usr/bin/bash
cd main
rm -r -f mr-* mr-out-*
cd ../mrapps && go build -race -buildmode=plugin wc.go
cd ../mrapps && go build -race -buildmode=plugin indexer.go
cd ../mrapps && go build -race -buildmode=plugin grep.go
cd ../main && go run -race mrcoordinator.go pg-*.txt