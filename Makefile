raft_test=cd src/raft && go test -race -run

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

# Raft
leader:
	$(raft_test) TestInitialElection2A
# Re-election
reel:
	$(raft_test) TestReElection2A
raftall:
	$(raft_test) 2A
# Agreement
b1:
	$(raft_test) TestBasicAgree2B
b2:
	$(raft_test) TestRPCBytes2B
b3:
	$(raft_test) TestFailAgree2B
b4:
	$(raft_test) TestFailNoAgree2B
b5:
	$(raft_test) TestConcurrentStarts2B
b6:
	$(raft_test) TestRejoin2B
b7:
	$(raft_test) TestBackup2B
append:
	$(raft_test) 2B
