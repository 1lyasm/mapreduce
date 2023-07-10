#!/usr/bin/bash
cd main
rm -r -f mr-* mr-out-*
cd ../mrapps && go build -buildmode=plugin wc.go
cd ../main && go run mrcoordinator.go pg*.txt