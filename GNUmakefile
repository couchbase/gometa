.PHONY: build
build: message/protocol.pb.go
	go build -x -v -o gometa ./cmd/gometa

message/protocol.pb.go: message/protocol.proto
	cd $(dir $@) && protoc --go_out=. $(notdir $^)

.PHONY: clean
clean:
	go clean
