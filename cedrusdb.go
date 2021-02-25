package cedrusdb

// #include <stdlib.h>
// #include "cedrusdb/db.h"
import "C"
import (
	"errors"
	"fmt"
	"os"
	"unsafe"
)

var (
	// ErrPut indicates an error during Put()
	ErrPut = errors.New("error from cedrus_put")
	// ErrPutByHash indicates an error during PutByHash()
	ErrPutByHash = errors.New("error from cedrus_put_by_hash")
	// ErrGet indicates an error during Get()
	ErrGet = errors.New("error from cedrus_get")
	// ErrGetMut indicates an error during GetMut()
	ErrGetMut = errors.New("error from cedrus_get_mut")
	// ErrDelete indicates an error during Delete()
	ErrDelete = errors.New("error from cedrus_delete")
	// ErrReplace indicates an error during Replace()
	ErrReplace = errors.New("error from cedrus_replace")
	// ErrGetByHash indicates an error during GetByHash()
	ErrGetByHash = errors.New("error from cedrus_get_by_hash")
	// ErrGetByHashMut indicates an error during GetByHashMut()
	ErrGetByHashMut = errors.New("error from cedrus_get_by_hash_mut")
	// ErrDeleteByHash indicates an error during DeleteByHash()
	ErrDeleteByHash = errors.New("error from cedrus_delete_by_hash")
	// ErrWriteBatchPut indicates an error during CedrusWriteBatch.Put()
	ErrWriteBatchPut = errors.New("error from cedrus_writebatch_put")
	// ErrWriteBatchPutByHash indicates an error during CedrusWriteBatch.PutByHash()
	ErrWriteBatchPutByHash = errors.New("error from cedrus_writebatch_put_by_hash")
	// ErrWriteBatchDelete indicates an error during CedrusWriteBatch.Delete()
	ErrWriteBatchDelete = errors.New("error from cedrus_writebatch_delete")
	// ErrWriteBatchDeleteByHash indicates an error during CedrusWriteBatch.DeleteByHash()
	ErrWriteBatchDeleteByHash = errors.New("error from cedrus_writebatch_delete_by_hash")
	// ErrWriteBatchWrite indicates an error during CedrusWriteBatch.Write()
	ErrWriteBatchWrite = errors.New("error from cedrus_writebatch_write")
	// ErrIntegrity indicates there is some integrity error
	ErrIntegrity = errors.New("found some integrity error in the db")
)

type rawPtr = unsafe.Pointer

func doubleFreeWarn(freed *bool) (res bool) {
	res = *freed
	if res {
		fmt.Fprintf(os.Stderr, "attempt to double free!\n")
	}
	*freed = true
	return
}

var dummyBytes = []byte{0}

func sliceAsBytes(s []byte) (*C.uint8_t, C.size_t) {
	if len(s) == 0 {
		return (*C.uint8_t)(&dummyBytes[0]), 0
	}
	return (*C.uint8_t)(&s[0]), (C.size_t)(len(s))
}

//// begin Cedrus def

type cCedrus = *C.Cedrus

// CedrusObj is the wrapped Cedrus pointer.
type CedrusObj struct {
	inner cCedrus
	freed bool
}

// Cedrus is the CedrusDB handle.
type Cedrus = *CedrusObj

// cedrusFromC converts an existing C pointer into a go object. Notice that the
// go object does *not* own the resource of the C pointer, so it is only valid
// to the extent in which the given C pointer is valid. The C memory will not
// be deallocated when the go object is finalized by GC. This applies to all
// other "FromC" functions.
func cedrusFromC(ptr cCedrus) Cedrus {
	return &CedrusObj{inner: ptr}
}

// Free closes the CedrusDB and free the underlying C pointer.
func (c Cedrus) Free() {
	if doubleFreeWarn(&c.freed) {
		return
	}
	C.cedrus_free(c.inner)
}

//// end Cedrus def

//// begin CedrusValueRef def

type cCedrusValueRef = *C.CedrusValueRef

// CedrusValueRefObj is the wrapped CedrusValueRef pointer.
type CedrusValueRefObj struct {
	inner cCedrusValueRef
	freed bool
}

// CedrusValueRef is a handle to the item's value in the store (by holding a
// reader lock).
type CedrusValueRef = *CedrusValueRefObj

func cedrusValueRefFromC(ptr cCedrusValueRef) CedrusValueRef {
	return &CedrusValueRefObj{inner: ptr}
}

// Free the underlying C pointer manually.
func (vr CedrusValueRef) Free() {
	if doubleFreeWarn(&vr.freed) {
		return
	}
	C.cedrus_vr_free(vr.inner)
}

//// end CedrusValueRef def

//// begin CedrusValueMut def

type cCedrusValueMut = *C.CedrusValueMut

// CedrusValueMutObj is the wrapped CedrusValueMut pointer.
type CedrusValueMutObj struct {
	inner cCedrusValueMut
	freed bool
}

// CedrusValueMut is a handle to the item's value in the store (by holding a
// writer lock).
type CedrusValueMut = *CedrusValueMutObj

func cedrusValueMutFromC(ptr cCedrusValueMut) CedrusValueMut {
	return &CedrusValueMutObj{inner: ptr}
}

// Free the underlying C pointer manually.
func (vm CedrusValueMut) Free() {
	if doubleFreeWarn(&vm.freed) {
		return
	}
	C.cedrus_vm_free(vm.inner)
}

//// end CedrusValueMut def

//// begin CedrusWriteBatch def

type cCedrusWriteBatch = *C.CedrusWriteBatch

// CedrusWriteBatchObj is the wrapped CedrusWriteBatch pointer.
type CedrusWriteBatchObj struct {
	inner cCedrusWriteBatch
	freed bool
}

// CedrusWriteBatch keeps a write batch.
type CedrusWriteBatch = *CedrusWriteBatchObj

func cedrusWriteBatchFromC(ptr cCedrusWriteBatch) CedrusWriteBatch {
	return &CedrusWriteBatchObj{inner: ptr}
}

// Drop aborts the current write batch. Do not call this function after a
// `Write` or an error from a write batch operation (`Put`, etc.) as the write
// batch is automatically freed in either case. Only call this function when
// the write batch has not had any error and you just would like to manually
// abort it.
func (wb CedrusWriteBatch) Drop() {
	if doubleFreeWarn(&wb.freed) {
		return
	}
	C.cedrus_writebatch_drop(wb.inner)
}

//// end CedrusWriteBatch def

// CedrusConfig is the configuration for creating/opening CedrusDB.
type CedrusConfig = C.CedrusConfig

// NewCedrus creates a CedrusDB handle. You should manually free the handle to
// close the DB after use by `Free`.
func NewCedrus(dbPath string, config *CedrusConfig, truncate bool) (res Cedrus) {
	dbPathStr := C.CString(dbPath)
	var trunc C.int
	if truncate {
		trunc = 1
	}
	res = cedrusFromC(C.cedrus_new(dbPathStr, config, trunc))
	C.free(rawPtr(dbPathStr))
	return
}

// DefaultConfig generates a default config for CedrusDB.
func DefaultConfig() CedrusConfig {
	return C.cedrus_config_default()
}

// AsBytes access the byte content of the value. One should *not* write to the
// byte slice.
func (vr CedrusValueRef) AsBytes() (bytes []byte) {
	vif := C.cedrus_vr_info(vr.inner)
	return C.GoBytes(rawPtr(vif.base), C.int(vif.size))
}

// AsBytes access the byte content of the value. One should *not* write to the
// byte slice.
func (vm CedrusValueMut) AsBytes() (bytes []byte) {
	vif := C.cedrus_vm_info(vm.inner)
	return C.GoBytes(rawPtr(vif.base), C.int(vif.size))
}

// Put (aka. insert/upsert) an item into the store with an arbitrary key. The
// first boolean is true if an existing item is updated.
func (c Cedrus) Put(key []byte, val []byte) (bool, error) {
	kPtr, kLen := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	ret := C.cedrus_put(c.inner, kPtr, kLen, vPtr, vLen)
	if ret < 0 {
		return false, ErrPut
	}
	return ret == 1, nil
}

// Get (aka. lookup/query) an item from the store with an arbitrary key.
//
// - On success, `vr` is assigned to a struct that could be used to read the
// value, which should be freed by `Free` after use.
func (c Cedrus) Get(key []byte) (vr CedrusValueRef, err error) {
	var vrc *C.CedrusValueRef
	kPtr, kLen := sliceAsBytes(key)
	ret := C.cedrus_get(c.inner, kPtr, kLen, &vrc)
	if ret == 0 {
		vr = cedrusValueRefFromC(vrc)
	} else {
		err = ErrGet
	}
	return
}

// GetMut gets (aka. lookup/query) an item from the store with an arbitrary key.
//
// - On success, `vm` is assigned to a struct that could be used to read/write
// the value, which should be freed by `Free` after use.
func (c Cedrus) GetMut(key []byte) (vm CedrusValueMut, err error) {
	var vmc *C.CedrusValueMut
	kPtr, kLen := sliceAsBytes(key)
	ret := C.cedrus_get_mut(c.inner, kPtr, kLen, &vmc)
	if ret == 0 {
		vm = cedrusValueMutFromC(vmc)
	} else {
		err = ErrGetMut
	}
	return
}

// Delete (aka. remove) an item from the store with an arbitrary key.
func (c Cedrus) Delete(key []byte) (err error) {
	kPtr, kLen := sliceAsBytes(key)
	if C.cedrus_delete(c.inner, kPtr, kLen) != 0 {
		err = ErrDelete
	}
	return
}

// Replace (aka. update) the value at the given `ValueMut`. This function
// simply replaces the value by copying and thus allows changing the value to
// arbitrary size. The `ValueMut` handle will always be consumed and freed so
// there is no need to call `Free`.
func (c Cedrus) Replace(vm CedrusValueMut, newValue []byte) (err error) {
	vPtr, vLen := sliceAsBytes(newValue)
	if C.cedrus_replace(c.inner, vm.inner, vPtr, vLen) != 0 {
		err = ErrReplace
	}
	return
}

// PutByHash puts (aka. insert/upsert) an item into the store, assuming the key
// is already a hash value. The first boolean is true if an existing item is
// updated.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) PutByHash(key []byte, val []byte) (bool, error) {
	kPtr, _ := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	ret := C.cedrus_put_by_hash(c.inner, kPtr, vPtr, vLen)
	if ret < 0 {
		return false, ErrPutByHash
	}
	return ret == 1, nil
}

// GetByHash gets (aka. lookup/query) an item from the store, assuming the key
// is already a hash value.
//
// - On success, `vr` is assigned to a struct that could be used to read the
// value, which should be freed by `Free` after use.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) GetByHash(key []byte) (vr CedrusValueRef, err error) {
	var vrc *C.CedrusValueRef
	kPtr, _ := sliceAsBytes(key)
	ret := C.cedrus_get_by_hash(c.inner, kPtr, &vrc)
	if ret == 0 {
		vr = cedrusValueRefFromC(vrc)
	} else {
		err = ErrGetByHash
	}
	return
}

// GetByHashMut gets (aka. lookup/query) an item from the store, assuming the
// key is already a hash value.
// - On success, `vm` is assigned to a struct that could be used to read/write
// the value, which should be freed by `Free` after use.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) GetByHashMut(key []byte) (vm CedrusValueMut, err error) {
	var vmc *C.CedrusValueMut
	kPtr, _ := sliceAsBytes(key)
	ret := C.cedrus_get_by_hash_mut(c.inner, kPtr, &vmc)
	if ret == 0 {
		vm = cedrusValueMutFromC(vmc)
	} else {
		err = ErrGetByHashMut
	}
	return
}

// DeleteByHash deletes (aka. remove) an item from the store, assuming the key
// is already a hash value.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) DeleteByHash(key []byte) (err error) {
	kPtr, _ := sliceAsBytes(key)
	if C.cedrus_delete_by_hash(c.inner, kPtr) != 0 {
		err = ErrDeleteByHash
	}
	return
}

// NewWriteBatch creates a write batch. A write batch allows batching more than
// one write operations (put/delete) together to make them atomic.
//
// Performance tips:
// - Some locks are grabbed once a new write batch is created. Thus, it is
// always a good practice to prepare the batched writes before creating the
// write batch, and then feed all of them in at once, followed by `Write`.
// - For a single write operation, directly using functions under
// `Cedrus` is more efficient than creating an unnecessary write
// batch. A write batch keeps its guarantees at some overhead.
// - A thread must drop (either by `Write`, or due to error in operations) the
// previous write batch before it creates the subsequent one.  Creating two
// write batches at once will result in a deadlock.
func (c Cedrus) NewWriteBatch() CedrusWriteBatch {
	return cedrusWriteBatchFromC(C.cedrus_writebatch_new(c.inner))
}

// Put pushes a `Put` operation to the write batch. The first boolean is true
// if an existing item is updated.
//
// - On failure, the write batch is automatically aborted and
// freed. The write batch pointer will become invalid and one should not use it
// with any other functions.
func (wb CedrusWriteBatch) Put(key []byte, val []byte) (err error) {
	kPtr, kLen := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	if C.cedrus_writebatch_put(wb.inner, kPtr, kLen, vPtr, vLen) != 0 {
		err = ErrWriteBatchPut
	}
	return
}

// Delete pushes a `Delete` operation to the write batch.
//
// - On failure, the write batch is automatically aborted and freed. The write
// batch pointer will become invalid and one should not use it with any other
// functions.
func (wb CedrusWriteBatch) Delete(key []byte) (err error) {
	kPtr, kLen := sliceAsBytes(key)
	if C.cedrus_writebatch_delete(wb.inner, kPtr, kLen) != 0 {
		err = ErrWriteBatchDelete
	}
	return
}

// PutByHash pushes a `PutByHash` operation to the write batch.
//
// - On failure, the write batch is automatically aborted and freed. The write
// batch pointer will become invalid and one should not use it with any other
// functions.
func (wb CedrusWriteBatch) PutByHash(key []byte, val []byte) (err error) {
	kPtr, _ := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	if C.cedrus_writebatch_put_by_hash(wb.inner, kPtr, vPtr, vLen) != 0 {
		err = ErrWriteBatchPutByHash
	}
	return
}

// DeleteByHash pushes a `DeleteByHash` operation to the write batch.
//
// - On failure, the write batch is automatically aborted and freed. The write
// batch pointer will become invalid and one should not use it with any other
// functions.
func (wb CedrusWriteBatch) DeleteByHash(key []byte) (err error) {
	kPtr, _ := sliceAsBytes(key)
	if C.cedrus_writebatch_delete_by_hash(wb.inner, kPtr) != 0 {
		err = ErrWriteBatchDeleteByHash
	}
	return
}

// Finalize and commit all operations in the write batch.
// - The write batch is always guaranteed to be freed.
func (wb CedrusWriteBatch) Write() (err error) {
	if C.cedrus_writebatch_write(wb.inner) != 0 {
		err = ErrWriteBatchWrite
	}
	return
}

// CheckIntegrity checks the integrity of the database. Note that this method
// is *not* thread-safe.
func (c Cedrus) CheckIntegrity() (err error) {
	if C.cedrus_check_integrity(c.inner) != 0 {
		err = ErrIntegrity
	}
	return
}
