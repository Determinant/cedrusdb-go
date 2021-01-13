Install
=======

- Download the package: ``go get -d github.com/Determinant/cedrusdb-go``.
- Set go path: ``export GOPATH=$(go env GOPATH)``.
- Fetch the binaries: ``$GOPATH/src/github.com/Determinant/cedrusdb-go/scripts/build.sh``.

How to Build Your Applicaiton
=============================

- Use ``source $GOPATH/src/github.com/Determinant/cedrusdb-go/scripts/env.sh && go build -o`` in place of ``go build -o``.

How to Build the Example Program
================================

- CD into the package: ``cd $GOPATH/src/github.com/Determinant/cedrusdb-go``.
- Build the example program: ``make``.
