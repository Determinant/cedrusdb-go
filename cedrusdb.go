package cedrusdb

// #include <stdlib.h>
// #include "cedrusdb/db.h"
import "C"
import "runtime"
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

type CedrusConfig = C.CedrusConfig

func NewCedrus(dbPath string, config *C.CedrusConfig, truncate bool, autoFree bool) (res Cedrus) {
	dbPathStr := C.CString(dbPath)
	var trunc C.int
	if truncate {
		trunc = 1
	}
	res = CedrusFromC(C.cedrus_new(dbPathStr, config, trunc))
	C.free(rawPtr(dbPathStr))
	cedrusSetFinalizer(res, autoFree)
	return
}

func DefaultConfig() CedrusConfig {
	return C.cedrus_config_default()
}

func (c Cedrus) Put(key []byte, val []byte) (res int) {
	return int(C.cedrus_put(c.inner, (*C.uint8_t)(&key[0]), (C.size_t)(len(key)), (*C.uint8_t)(&val[0]), (C.size_t)(len(val))))
}

//// end Cedrus def
