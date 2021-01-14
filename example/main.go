package main

import (
	"fmt"
	"github.com/Determinant/cedrusdb-go"
)

func main() {
	cfg := cedrusdb.DefaultConfig()
	db := cedrusdb.NewCedrus("./testdb", &cfg, true)
	fmt.Printf("Put = %d\n", db.Put([]byte("hello"), []byte("world")))
	_, vr := db.Get([]byte("hello"))
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()
	fmt.Printf("Put = %d\n", db.Put([]byte("hello1"), []byte("world")))
	fmt.Printf("Delete = %d\n", db.Delete([]byte("hello1")))
	fmt.Printf("Delete = %d\n", db.Delete([]byte("hello1")))
	db.Free()
}
