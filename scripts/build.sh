#!/bin/bash -e

SRC_DIR="$(dirname "${BASH_SOURCE[0]}")"

source "${SRC_DIR}/env.sh"

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    cd "$CEDRUSDB_PATH"
    wget "https://github.com/Determinant/cedrusdb-bin/raw/master/x86-64/libcedrusdb.so.tar.xz"
    tar xvf libcedrusdb.so.tar.xz
else
    echo "Only Linux system is supported by CedrusDB."
    exit 1
fi
