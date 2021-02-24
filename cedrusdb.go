package cedrusdb

// #include <stdlib.h>
// #include "cedrusdb/db.h"
import "C"
import (
	"fmt"
	"os"
	"unsafe"
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

// Put (aka. insert/upsert) an item into the store with an arbitrary key.  -
// Returns -1 on failure, 0 if item is inserted, 1 if an existing item is
// updated. TODO: more detailed error code.
func (c Cedrus) Put(key []byte, val []byte) int {
	kPtr, kLen := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	return int(C.cedrus_put(c.inner, kPtr, kLen, vPtr, vLen))
}

// Get (aka. lookup/query) an item from the store with an arbitrary key.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
// - `vr` is assigned to a struct that could be used to read the value, which
// should be freed by `Free` after use.
func (c Cedrus) Get(key []byte) (res int, vr CedrusValueRef) {
	var vrc *C.CedrusValueRef
	kPtr, kLen := sliceAsBytes(key)
	res = int(C.cedrus_get(c.inner, kPtr, kLen, &vrc))
	if res == 0 {
		vr = cedrusValueRefFromC(vrc)
	}
	return
}

// GetMut gets (aka. lookup/query) an item from the store with an arbitrary key.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
// - `vm` is assigned to a struct that could be used to read/write the value,
// which should be freed by `Free` after use.
func (c Cedrus) GetMut(key []byte) (res int, vm CedrusValueMut) {
	var vmc *C.CedrusValueMut
	kPtr, kLen := sliceAsBytes(key)
	res = int(C.cedrus_get_mut(c.inner, kPtr, kLen, &vmc))
	if res == 0 {
		vm = cedrusValueMutFromC(vmc)
	}
	return
}

// Delete (aka. remove) an item from the store with an arbitrary key.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
func (c Cedrus) Delete(key []byte) int {
	kPtr, kLen := sliceAsBytes(key)
	return int(C.cedrus_delete(c.inner, kPtr, kLen))
}

// Replace (aka. update) the value at the given `ValueMut`. This function
// simply replaces the value by copying and thus allows changing the value to
// arbitrary size. The `ValueMut` handle will always be consumed and freed so
// there is no need to call `Free`.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
func (c Cedrus) Replace(vm CedrusValueMut, newValue []byte) int {
	vPtr, vLen := sliceAsBytes(newValue)
	return int(C.cedrus_replace(c.inner, vm.inner, vPtr, vLen))
}

// PutByHash puts (aka. insert/upsert) an item into the store, assuming the key
// is already a hash value.
// - Returns -1 on failure, 0 if item is inserted, 1 if an existing item is
// updated. TODO: more detailed error code.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) PutByHash(key []byte, val []byte) int {
	kPtr, _ := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	return int(C.cedrus_put_by_hash(c.inner, kPtr, vPtr, vLen))
}

// GetByHash gets (aka. lookup/query) an item from the store, assuming the key
// is already a hash value.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
// - `vr` is assigned to a struct that could be used to read the value, which
// should be freed by `Free` after use.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) GetByHash(key []byte) (res int, vr CedrusValueRef) {
	var vrc *C.CedrusValueRef
	kPtr, _ := sliceAsBytes(key)
	res = int(C.cedrus_get_by_hash(c.inner, kPtr, &vrc))
	if res == 0 {
		vr = cedrusValueRefFromC(vrc)
	}
	return
}

// GetByHashMut gets (aka. lookup/query) an item from the store, assuming the
// key is already a hash value.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
// - `vm` is assigned to a struct that could be used to read/write the value,
// which should be freed by `Free` after use.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) GetByHashMut(key []byte) (res int, vm CedrusValueMut) {
	var vmc *C.CedrusValueMut
	kPtr, _ := sliceAsBytes(key)
	res = int(C.cedrus_get_by_hash_mut(c.inner, kPtr, &vmc))
	if res == 0 {
		vm = cedrusValueMutFromC(vmc)
	}
	return
}

// DeleteByHash deletes (aka. remove) an item from the store, assuming the key
// is already a hash value.  - Returns 0 on success, -1 on failure. TODO: more
// detailed error code.
//
// Note: By assuming the key is already a hash value, CedrusDB will no longer
// hash the key. The length is by default 32 bytes, but could be 8/16 bytes if
// Rust code is built with `hash64`/`hash128`.
func (c Cedrus) DeleteByHash(key []byte) int {
	kPtr, _ := sliceAsBytes(key)
	return int(C.cedrus_delete_by_hash(c.inner, kPtr))
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

// Put pushes a `Put` operation to the write batch.
// - Returns 0 on success.
// When it returns non-zero value, the write batch is automatically aborted and
// freed. The write batch pointer will become invalid and one should not use it
// with any other functions.
func (wb CedrusWriteBatch) Put(key []byte, val []byte) int {
	kPtr, kLen := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	return int(C.cedrus_writebatch_put(wb.inner, kPtr, kLen, vPtr, vLen))
}

// Delete pushes a `Delete` operation to the write batch.
// - Returns 0 on success. When it returns non-zero value, the write batch is
// automatically aborted and freed. The write batch pointer will become invalid
// and one should not use it with any other functions.
func (wb CedrusWriteBatch) Delete(key []byte) int {
	kPtr, kLen := sliceAsBytes(key)
	return int(C.cedrus_writebatch_delete(wb.inner, kPtr, kLen))
}

// PutByHash pushes a `PutByHash` operation to the write batch.
// - Returns 0 on success. When it returns non-zero value, the write batch is
// automatically aborted and freed. The write batch pointer will become invalid
// and one should not use it with any other functions.
func (wb CedrusWriteBatch) PutByHash(key []byte, val []byte) int {
	kPtr, _ := sliceAsBytes(key)
	vPtr, vLen := sliceAsBytes(val)
	return int(C.cedrus_writebatch_put_by_hash(wb.inner, kPtr, vPtr, vLen))
}

// DeleteByHash pushes a `DeleteByHash` operation to the write batch.
// - Returns 0 on success. When it returns non-zero value, the write batch is
// automatically aborted and freed. The write batch pointer will become invalid
// and one should not use it with any other functions.
func (wb CedrusWriteBatch) DeleteByHash(key []byte) int {
	kPtr, _ := sliceAsBytes(key)
	return int(C.cedrus_writebatch_delete_by_hash(wb.inner, kPtr))
}

// Finalize and commit all operations in the write batch.
// - Returns 0 on success. The write batch is always guaranteed to be freed.
func (wb CedrusWriteBatch) Write() int {
	return int(C.cedrus_writebatch_write(wb.inner))
}

// CheckIntegrity checks the integrity of the database. Note that this method
// is *not* thread-safe.
// - Returns 0 on success, -1 on failure. TODO: more detailed error code.
func (c Cedrus) CheckIntegrity() int {
	return int(C.cedrus_check_integrity(c.inner))
}
