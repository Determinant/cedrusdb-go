.PHONY: all clean cdep examples

all: cdep examples

examples: example-app

example-app: example/main.go
	scripts/build.sh
	bash -c 'source $$(go env GOPATH)/src/github.com/Determinant/cedrusdb-go/scripts/env.sh && go build -o $@ github.com/Determinant/cedrusdb-go/example'

clean:
	rm -rf build/
	scripts/clean.sh
