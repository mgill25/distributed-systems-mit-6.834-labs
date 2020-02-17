FROM golang:1.13.6-stretch

WORKDIR $GOPATH/src/github.com/mit/labs

COPY . .

RUN cd src/main
RUN go build -buildmode=plugin ../mrapps/wc.go
RUN rm mr-out*
RUN go run mrsequential.go wc.so pg*.txt
RUN more mr-out-0
