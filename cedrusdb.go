package cedrusdb

// #include <stdlib.h>
// #include "cedrusdb/db.h"
import "C"
import (
	"fmt"
	"os"
	"runtime"
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

//// begin Cedrus def

// CCedrus is the C pointer type for a NetAddr object
type CCedrus = *C.Cedrus
type cedrus struct {
	inner    CCedrus
	autoFree bool
	freed    bool
}

type Cedrus = *cedrus

// CedrusFromC converts an existing C pointer into a go object. Notice that
// when the go object does *not* own the resource of the C pointer, so it is
// only valid to the extent in which the given C pointer is valid. The C memory
// will not be deallocated when the go object is finalized by GC. This applies
// to all other "FromC" functions.
func CedrusFromC(ptr CCedrus) Cedrus {
	return &cedrus{inner: ptr}
}

func cedrusSetFinalizer(res Cedrus, autoFree bool) {
	res.autoFree = autoFree
	if res.inner != nil && autoFree {
		runtime.SetFinalizer(res, func(self Cedrus) { self.Free() })
	}
}

// Free the underlying C pointer manually.
func (c Cedrus) Free() {
	if doubleFreeWarn(&c.freed) {
		return
	}
	C.cedrus_free(c.inner)
	if c.autoFree {
		runtime.SetFinalizer(c, nil)
	}
}

//// end Cedrus def

//// begin CedrusValueRef def

// CCedrusValueRef is the C pointer type for a NetAddr object
type CCedrusValueRef = *C.CedrusValueRef
type cedrusValueRef struct {
	inner    CCedrusValueRef
	autoFree bool
	freed    bool
}

type CedrusValueRef = *cedrusValueRef

// CedrusValueRefFromC converts an existing C pointer into a go object. Notice that
// when the go object does *not* own the resource of the C pointer, so it is
// only valid to the extent in which the given C pointer is valid. The C memory
// will not be deallocated when the go object is finalized by GC. This applies
// to all other "FromC" functions.
func CedrusValueRefFromC(ptr CCedrusValueRef) CedrusValueRef {
	return &cedrusValueRef{inner: ptr}
}

func cedrusValueRefSetFinalizer(res CedrusValueRef, autoFree bool) {
	res.autoFree = autoFree
	if res.inner != nil && autoFree {
		runtime.SetFinalizer(res, func(self CedrusValueRef) { self.Free() })
	}
}

// Free the underlying C pointer manually.
func (c CedrusValueRef) Free() {
	if doubleFreeWarn(&c.freed) {
		return
	}
	C.cedrus_free_vr(c.inner)
	if c.autoFree {
		runtime.SetFinalizer(c, nil)
	}
}

//// end CedrusValueRef def

type CedrusConfig = C.CedrusConfig

func NewCedrus(dbPath string, config *C.CedrusConfig, truncate bool) (res Cedrus) {
	dbPathStr := C.CString(dbPath)
	var trunc C.int
	if truncate {
		trunc = 1
	}
	res = CedrusFromC(C.cedrus_new(dbPathStr, config, trunc))
	C.free(rawPtr(dbPathStr))
	cedrusSetFinalizer(res, true)
	return
}

func DefaultConfig() CedrusConfig {
	return C.cedrus_config_default()
}

func (c Cedrus) Put(key []byte, val []byte) int {
	return int(C.cedrus_put(
		c.inner,
		(*C.uint8_t)(&key[0]), (C.size_t)(len(key)),
		(*C.uint8_t)(&val[0]), (C.size_t)(len(val))))
}

func (c Cedrus) Get(key []byte) (res int, vr CedrusValueRef) {
	var vr_ *C.CedrusValueRef
	res = int(C.cedrus_get(c.inner, (*C.uint8_t)(&key[0]), (C.size_t)(len(key)), &vr_))
	if res == 0 {
		vr = CedrusValueRefFromC(vr_)
		cedrusValueRefSetFinalizer(vr, false)
	}
	return
}

func (c Cedrus) Delete(key []byte) int {
	return int(C.cedrus_delete(
		c.inner,
		(*C.uint8_t)(&key[0]), (C.size_t)(len(key))))
}

func (vr CedrusValueRef) AsBytes() (bytes []byte) {
	vif := C.cedrus_vr_info(vr.inner)
	return C.GoBytes(rawPtr(vif.base), C.int(vif.size))
}
