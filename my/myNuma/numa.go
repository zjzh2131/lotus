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

// NumaRunOnNode equal numa_run_on_node()
func NumaRunOnNode(node int) {
	C.numa_run_on_node(C.int(node))
}

// NumaNumTaskNodes equal numa_num_task_nodes()
//func NumaNumTaskNodes() int {
//	return C.numa_num_task_nodes
//}

// NumaNumTaskCpus equal numa_num_task_cpus()
//func NumaNumTaskCpus() int {
//	return C.numa_num_task_cpus
//}

// numa_node_size, numa_node_size64

// NumaNodeSize64 equal numa_node_size64
//func NumaNodeSize64(node int, free bool) int64 {
//	freeP := (*C.longlong)((unsafe.Pointer)(new(int64)))
//	return C.numa_node_size64(C.int(node), freeP)
//}

type NodeMask C.nodemask_t

// NumaBind equal numa_bind()
func NumaBind(node int) {
	maskP := new(C.nodemask_t)
	C.nodemask_zero(maskP)
	C.nodemask_set_compat(maskP, C.int(node))
	C.numa_bind_compat(maskP)
}
