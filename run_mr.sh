#!/bin/bash

cd src/main || return
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
# go run mrsequential.go wc.so pg*.txt
# more mr-out-0
