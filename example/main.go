package main

import (
	"fmt"
	"github.com/Determinant/cedrusdb-go"
)

func assertOk(ret int) {
	if ret != 0 {
		panic("assertion failed")
	}
}

func assertErr(ret int) {
	if ret == 0 {
		panic("assertion failed")
	}
}

func main() {
	hashKey := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	hashKey2 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxy"
	config := cedrusdb.DefaultConfig()
	db := cedrusdb.NewCedrus("testdb", &config, true)

	assertOk(db.Put([]byte("hello"), []byte("world")))
	assertOk(db.Put([]byte("happy"), []byte("ted")))
	assertOk(db.Put([]byte("useless"), []byte("value")))

	ret, vr := db.Get([]byte("hello"))
	assertOk(ret)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	assertOk(db.Delete([]byte("useless")))
	assertErr(db.Delete([]byte("useless")))

	assertOk(db.PutByHash([]byte(hashKey), []byte("yyyy")))
	assertOk(db.PutByHash([]byte(hashKey2), []byte("zzzz")))
	ret, vr = db.GetByHash([]byte(hashKey))
	assertOk(ret)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	assertOk(db.DeleteByHash([]byte(hashKey2)))
	assertErr(db.DeleteByHash([]byte(hashKey2)))

	ret, vm := db.GetMut([]byte("hello"))
	assertOk(ret)
	fmt.Printf("%s\n", string(vm.AsBytes()))
	assertOk(db.Replace(vm, []byte("worl*")))

	ret, vm = db.GetByHashMut([]byte(hashKey))
	assertOk(ret)
	fmt.Printf("%s\n", string(vm.AsBytes()))
	assertOk(db.Replace(vm, []byte("longer value")))

	ret, vr = db.GetByHash([]byte([]byte(hashKey)))
	assertOk(ret)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	ret, vr = db.Get([]byte("hello"))
	assertOk(ret)
	fmt.Printf("%s\n", string(vr.AsBytes()))
	vr.Free()

	wb := db.NewWriteBatch()
	assertOk(wb.Put([]byte("world"), []byte("hello")))
	assertOk(wb.Put([]byte("ted"), []byte("happy")))
	assertOk(wb.Write())

	db.Free()

	db = cedrusdb.NewCedrus("testdb", &config, false)

	ret, vr = db.Get([]byte("ted"))
	assertOk(ret)
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
