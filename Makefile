all:
	go build -o raft-demo *.go

clean:
	rm -rf raft-demo

.PHONY: all clean