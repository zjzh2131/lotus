package sectorstorage

import (
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/my/db/myMongo"
	"github.com/filecoin-project/lotus/my/myUtils"
	"go.mongodb.org/mongo-driver/bson"
	"sort"
	"sync"
	"sync/atomic"
)

type NeedResource struct {
	cpuCount int
	task     taskReq
}

type BoundResource struct {
	cpus   []int
	nodeId int
}

type MyResourceScheduler struct {
	lk       sync.Mutex
	numaSche *numaSche
	gpuSche  *gpuSche
}

func NewMyRS() *MyResourceScheduler {
	return &MyResourceScheduler{
		lk:       sync.Mutex{},
		numaSche: initNumaSche(),
	}
}

func (rs *MyResourceScheduler) Lock() {
	rs.lk.Lock()
}

func (rs *MyResourceScheduler) Unlock() {
	rs.lk.Unlock()
}

func (rs *MyResourceScheduler) GetNuma(task taskReq, needCpu int) (bound BoundResource, freed func(), ok bool) {
	need := NeedResource{
		cpuCount: needCpu,
		task:     task,
	}
	return rs.numaSche.occupy(need)
}

func (rs *MyResourceScheduler) String() {
	for _, v := range rs.numaSche.nodeSche {
		fmt.Printf("v:%#v, running: %#v, used: %d\n", v, v.cpus.running, v.usedMem)
	}
}

type numaSche struct {
	nodeSche nodeSet
}

func initNumaSche() *numaSche {
	ns := &numaSche{
		nodeSche: nodeSet{},
	}
	workerMachine, err := myMongo.FindOneMachine(bson.M{
		"ip":   myUtils.GetLocalIPv4s(),
		"role": "worker",
	})
	if err != nil {
		panic("init numaSche failed")
	}
	for _, v := range workerMachine.HardwareInfo.NUMASet {
		tmp := &node{
			nodeId: int(v.NodeID),
			cpus: &cpuInfo{
				lk:      sync.Mutex{},
				pending: make(chan int, len(v.Cpus)),
				running: make(map[int]struct{}, len(v.Cpus)),
			},
			totalMem: v.TotalSize,
			usedMem:  0,
		}
		for _, cpuId := range v.Cpus {
			tmp.cpus.pending <- int(cpuId)
		}
		ns.nodeSche = append(ns.nodeSche, tmp)
	}
	return ns
}

func (ns nodeSet) sort() {
	// memory sort
	sort.SliceStable(ns, func(i, j int) bool {
		return ns[i].totalMem-ns[i].usedMem > ns[j].totalMem-ns[j].usedMem
	})

	// cpu sort
	sort.SliceStable(ns, func(i, j int) bool {
		return len(ns[i].cpus.pending) > len(ns[j].cpus.pending)
	})
}

func (ns *numaSche) occupy(need NeedResource) (bound BoundResource, freed func(), ok bool) {
	// step1 sort
	ns.nodeSche.sort()
	var selectedNode *node
	selectedNode = ns.nodeSche[0]
	// find min node
	//for _, v := range ns.nodeSche {
	//
	//}
	// step2 get
	boundCpu, cpuFreed, ok := selectedNode.cpus.occupy(need)
	if !ok {
		return BoundResource{nodeId: -1}, func() {}, false
	}
	//boundMem, memFreed, ok := selectedNode.occupy(need)
	//if !ok {
	//	cpuFreed()
	//	return BoundResource{nodeId: -1}, func() {}, false
	//}
	return BoundResource{
			cpus:   boundCpu.cpus,
			nodeId: selectedNode.nodeId,
		}, func() {
			cpuFreed()
			//memFreed()
		}, true
}

type nodeSet []*node

type node struct {
	nodeId   int
	cpus     *cpuInfo
	totalMem uint64
	usedMem  uint64
}

func (n *node) occupy(need NeedResource) (bound BoundResource, freed func(), ok bool) {
	maxMem := storiface.ResourceTable[sealtasks.TaskType(need.task.TaskType)][need.task.SectorRef.ProofType].MaxMemory
	if n.totalMem-n.usedMem < maxMem {
		fmt.Println(n.totalMem, n.usedMem, maxMem)
		fmt.Println("here")
		return BoundResource{}, func() {}, false
	}
	atomic.AddUint64(&n.usedMem, maxMem)
	return BoundResource{
			cpus:   nil,
			nodeId: n.nodeId,
		}, func() {
			fmt.Println("memory freed")
			atomic.AddUint64(&n.usedMem, ^(maxMem - 1))
		}, true
}

type cpuInfo struct {
	lk      sync.Mutex
	pending chan int
	running map[int]struct{}
}

func (c *cpuInfo) occupy(need NeedResource) (bound BoundResource, freed func(), ok bool) {
	if len(c.pending) < need.cpuCount {
		return BoundResource{}, func() {}, false
	}
	c.lk.Lock()
	defer c.lk.Unlock()
	var tmpCpus []int
	for i := 0; i < need.cpuCount; i++ {
		cpu := <-c.pending
		tmpCpus = append(tmpCpus, cpu)
		c.running[cpu] = struct{}{}
	}
	return BoundResource{
			cpus:   tmpCpus,
			nodeId: 0,
		}, func() {
			fmt.Println("cpu freed")
			c.lk.Lock()
			defer c.lk.Unlock()
			for _, v := range tmpCpus {
				c.pending <- v
				delete(c.running, v)
			}
		}, true
}

type gpuSche struct {
}

func (gs *gpuSche) occupy(need NeedResource) (bound BoundResource, freed func(), ok bool) {
	return BoundResource{}, nil, true
}
