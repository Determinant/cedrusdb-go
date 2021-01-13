package main

import "C"
import "github.com/Determinant/cedrusdb-go"

func main() {
	cfg := cedrusdb.DefaultConfig()
	db := cedrusdb.NewCedrus("./testdb", &cfg, true, false)
	db.Put([]byte("hello"), []byte("world"))
	db.Free()
}
