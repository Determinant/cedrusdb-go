#!/bin/bash -e

SRC_DIR="$(dirname "${BASH_SOURCE[0]}")"

source "${SRC_DIR}/env.sh"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    ARCH="$(uname -m)"
    if [[ "$ARCH" != "x86_64" ]]; then
        echo "Architecture not supported yet."
        exit 1
    fi
    if [[ ! "$SRC_DIR/../" -ef "$CEDRUSDB_GO_PATH" ]]; then
        echo "The script is not in the go path repo ($SRC_DIR != $CEDRUSDB_GO_PATH)"
        exit 1
    fi
    mkdir -p "$CEDRUSDB_PATH"
    cd "$CEDRUSDB_PATH"
    if [[ ! -f ./lib/libcedrusdb.a ]]; then
        mkdir -p "./lib"
        curl -sL "https://github.com/Determinant/cedrusdb-bin/raw/$CEDRUSDB_BIN_REV/x86-64/libcedrusdb.a.tar.xz" -o libcedrusdb.a.tar.xz
        tar xf libcedrusdb.a.tar.xz -C "./lib"
        rm libcedrusdb.a.tar.xz
    fi
    if [[ ! -d ./include ]]; then
        mkdir -p include/cedrusdb
        wget "https://github.com/Determinant/cedrusdb-bin/raw/$CEDRUSDB_BIN_REV/include/cedrusdb/db.h" -O include/cedrusdb/db.h
    fi
else
    echo "Only Linux system is supported by CedrusDB."
    exit 1
fi
