.PHONY: build
build:
	cd src/main && go build -buildmode=plugin ../mrapps/wc.go

clean:
	cd src/main && rm mr-out*

cleanup:
	cd src/main && rm mr-*-*

_master:
	cd src/main && go run mrmaster.go pg-*.txt

master: build _master

worker:
	cd src/main && go run mrworker.go wc.so

raft:
	cd src/raft && go test -run TestInitialElection2A -race

# Re-election
reel:
	cd src/raft && go test -run TestReElection2A -race
