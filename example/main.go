package main

import (
	"fmt"
	"github.com/Determinant/cedrusdb-go"
)

func assertOk(ret error) {
	if ret != nil {
		panic("assertion failed")
	}
}

func assertErr(ret error) {
	if ret == nil {
		panic("assertion failed")
	}
}

func main() {
	hashKey := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	hashKey2 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy"
	config := cedrusdb.DefaultConfig()
	db := cedrusdb.NewCedrus("testdb", &config, true)

	var err error
	_, err = db.Put([]byte("hello"), []byte("world"))
	assertOk(err)
	_, err = db.Put([]byte("happy"), []byte("ted"))
	assertOk(err)
	_, err = db.Put([]byte("useless"), []byte("value"))
	assertOk(err)

	vr, err := db.Get([]byte("hello"))
	assertOk(err)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	assertOk(db.Delete([]byte("useless")))
	assertErr(db.Delete([]byte("useless")))

	_, err = db.PutByHash([]byte(hashKey), []byte("yyyy"))
	assertOk(err)
	_, err = db.PutByHash([]byte(hashKey2), []byte("zzzz"))
	assertOk(err)
	vr, err = db.GetByHash([]byte(hashKey))
	assertOk(err)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	assertOk(db.DeleteByHash([]byte(hashKey2)))
	assertErr(db.DeleteByHash([]byte(hashKey2)))

	vm, err := db.GetMut([]byte("hello"))
	assertOk(err)
	fmt.Printf("%s\n", string(vm.AsBytes()))
	assertOk(db.Replace(vm, []byte("worl*")))

	vm, err = db.GetByHashMut([]byte(hashKey))
	assertOk(err)
	fmt.Printf("%s\n", string(vm.AsBytes()))
	assertOk(db.Replace(vm, []byte("longer value")))

	vr, err = db.GetByHash([]byte([]byte(hashKey)))
	assertOk(err)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	vr, err = db.Get([]byte("hello"))
	assertOk(err)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	wb := db.NewWriteBatch()
	assertOk(wb.Put([]byte("world"), []byte("hello")))
	assertOk(wb.Put([]byte("ted"), []byte("happy")))
	assertOk(wb.Write())

	db.Free()

	db = cedrusdb.NewCedrus("testdb", &config, false)

	vr, err = db.Get([]byte("ted"))
	assertOk(err)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	wb = db.NewWriteBatch()
	assertOk(wb.Delete([]byte("hello")))
	assertOk(wb.Write())

	wb = db.NewWriteBatch()
	assertOk(wb.PutByHash([]byte(hashKey2), []byte("....")))
	assertOk(wb.DeleteByHash([]byte(hashKey2)))
	assertOk(wb.Write())

	wb = db.NewWriteBatch()
	wb.Drop()

	assertOk(db.CheckIntegrity())
	db.Free()

}
