#!/usr/bin/bash
cd main
rm -r -f mr-* mr-out-*
cd ../mrapps && go build -race -buildmode=plugin wc.go
cd ../main && go run -race mrcoordinator.go test.txt