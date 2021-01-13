.PHONY: all clean cdep examples

all: cdep examples

examples: build/example

cdep: build/libcedrusdb.a

build/libcedrusdb.a:
	scripts/build.sh

build/example: build/libcedrusdb.a example/main.go
	bash -c 'source $$(go env GOPATH)/src/github.com/Determinant/cedrusdb-go/scripts/env.sh && go build -o $@ github.com/Determinant/cedrusdb-go/example'

clean:
	rm -rf build/
	scripts/clean.sh
