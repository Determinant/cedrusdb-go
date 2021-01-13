export GOPATH="$(go env GOPATH)"
export CEDRUSDB_ORG="Determinant"
export CEDRUSDB_GO_VER="v0.1.0"
export CEDRUSDB_GO_PATH="$GOPATH/src/github.com/$CEDRUSDB_ORG/cedrusdb-go"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    export CEDRUSDB_PATH="$CEDRUSDB_GO_PATH/cedrusdb"
    export CGO_CFLAGS="-I$CEDRUSDB_PATH/build/include/"
    export CGO_LDFLAGS="-L$CEDRUSDB_PATH/build/lib/ -lcedrusdb -g"
else
    echo "Only Linux system is supported by CedrusDB."
    exit 1
fi
