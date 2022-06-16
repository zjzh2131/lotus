package myNuma

// #cgo LDFLAGS: -lnuma
//
// #include <numa.h>
import "C"

// IsAvailable returns true if NUMA is
// available.
func IsAvailable() bool {
	return int(C.numa_available()) != -1
}

// MaxNode returns the highest node number
// available on this machine.
func MaxNode() int {
	return int(C.numa_max_node())
}

// Preferred returns the preferred node
// of the current thread.
func Preferred() int {
	return int(C.numa_preferred())
}

// SetPreferred sets the
// preferred node of the current thread.
func SetPreferred(node int) {
	C.numa_set_preferred(C.int(node))
}

// SetLocalAlloc sets the allocation
// to be made from the local memory.
func SetLocalAlloc() {
	C.numa_set_localalloc()
}
