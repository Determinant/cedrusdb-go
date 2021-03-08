export GOPATH="$(go env GOPATH)"
export CEDRUSDB_GO_VER="v0.1.3"
export CEDRUSDB_BIN_REV="v0.3.4"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    export CEDRUSDB_PATH="$HOME/.cache/cedrusdb-bin/"
    mkdir -p "$CEDRUSDB_PATH"
    export CGO_CFLAGS="-I$CEDRUSDB_PATH/include/"
    export CGO_LDFLAGS="-L$CEDRUSDB_PATH/lib/ -lcedrusdb -lrt -ldl"
else
    echo "Only Linux system is supported by CedrusDB."
    exit 1
fi
