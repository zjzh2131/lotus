package migration

import (
	"context"
	//"github.com/filecoin-project/lotus/extern/sector-storage/config"
	//"github.com/filecoin-project/lotus/extern/sector-storage/service/db"
	//"github.com/filecoin-project/lotus/extern/sector-storage/service/mod"
	//logging "github.com/ipfs/go-log/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/xerrors"

	//"os"
	"fmt"
	"sync"
	"time"
)

//var log = logging.Logger("StoreMachine")

var (
	MongoHandler *mongo.Database
)

var (
	DiskIOBalance        = "DiskIOBalance"
	NetWorkIOBalance     = "NetWorkIOBalance"
	ComprehensiveBalance = "ComprehensiveBalance"
	StoreMachineManager  = StoreMachineHandle{}
)

type StoreMachines struct {
	ID                           primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId
	StoreIP                      string             `json:"ip" bson:"ip"`
	StorePath                    string             `json:"storage_path" bson:"storage_path"`
	GroupID                      string             `json:"cluster_id" bson:"cluster_id"`
	MaxParallelMigrateSectorSize int32              `json:"MaxParallelMigrateSectorSize" bson:"MaxParallelMigrateSectorSize"`
	ParallelMigrateSectorSize    int32              `json:"parallelmigratesectorsize" bson:"parallelmigratesectorsize"`
	MaxStoreSectorSize           int32              `json:"MaxStoreSectorSize" bson:"MaxStoreSectorSize"`
	StoreSectorSize              int32              `json:"storesectorsize" bson:"storesectorsize"`
	Status                       int32              `json:"Status" bson:"Status"` // 1 usable  0 unusable
	FtpEnv                       FtpEnvStruct
	CreatedAt                    time.Time `json:"CreatedAt" bson:"CreatedAt"`
	UpdatedAt                    time.Time `json:"UpdatedAt" bson:"UpdatedAt"`
}

var (
	machines map[string]StoreMachines
)

type StoreMachineHandle struct {
	Handler   map[string]*CycleQueue //key: groupid
	MutexLock sync.Mutex
}

type CycleQueue struct {
	Data  []interface{}
	Front int
	Rear  int
	Cap   int
}

func NewCycleQueue(Cap int) *CycleQueue {
	return &CycleQueue{
		Data:  make([]interface{}, Cap),
		Cap:   Cap,
		Front: 0,
		Rear:  0,
	}
}

func (q *CycleQueue) Push(Data interface{}) bool {
	//check queue is full
	if (q.Rear+1)%q.Cap == q.Front {
		return false
	}
	q.Data[q.Rear] = Data
	q.Rear = (q.Rear + 1) % q.Cap
	return true
}

func (q *CycleQueue) Pop() interface{} {
	if q.Rear == q.Front {
		return nil
	}
	Data := q.Data[q.Front]
	q.Data[q.Front] = nil
	q.Front = (q.Front + 1) % q.Cap
	return Data
}

func (q *CycleQueue) Length() int {
	return (q.Rear - q.Front + q.Cap) % q.Cap
}

func (q *CycleQueue) Size() int {
	return q.Cap
}

func (q *CycleQueue) Head() int {
	return q.Front
}

func (q *CycleQueue) RearF() int {
	return q.Rear
}

func (q *CycleQueue) DataArray() []interface{} {
	return q.Data
}

func (q *CycleQueue) DataIndex(target interface{}) int {
	for i := q.Front; i < q.Rear; i++ {
		if q.Data[i] == target {
			return i
		}
	}
	return -1
}

func (q *CycleQueue) IndexDataChange(index int, target interface{}) bool {
	if index >= q.Cap {
		return false
	}
	q.Data[index] = target
	return true
}

func (q *CycleQueue) IsEmpty() bool {
	if q.Front == q.Rear {
		return true
	} else {
		return false
	}
}

func (smh *StoreMachineHandle) NewStoreMachines(ctx context.Context) error {
	smh.MutexLock.Lock()
	defer smh.MutexLock.Unlock()
	log.Info("NewStoreMachines ")
	f := bson.D{{"Status", 1}, {"role", "storage"}, {"ip", bson.D{{"$ne", "127.0.0.1"}}}}
	rs, err := MongoHandler.Collection("machines").Find(ctx, f)
	if err != nil {
		log.Error("find error ")
		return err
	}
	defer rs.Close(ctx)
	var sms []*StoreMachines
	if err := rs.All(ctx, &sms); err != nil {
		log.Error("rs.All error ")
		return err
	}
	//for _, result := range results {
	//	fmt.Println(result)
	//}
	if len(sms) == 0 {
		return xerrors.New("no machine can store")
	}

	fmt.Println("len(sms): ", len(sms))
	smhMap := map[string]*CycleQueue{}
	storeMachinesMap := map[string][]*StoreMachines{}
	for _, v := range sms {
		groupidStr := v.GroupID
		if v.MaxStoreSectorSize <= v.StoreSectorSize && v.ParallelMigrateSectorSize == 0 {
			continue
		}
		storeMachinesMap[groupidStr] = append(storeMachinesMap[groupidStr], v)

		//fmt.Println("1111: ", v.ID)
		//fmt.Println("1111: ", v.StoreIP)
		//fmt.Println("1111: ", v.StorePath)
		//fmt.Println("1111: ", v.GroupID)
		//fmt.Println("1111: ", v.MaxParallelMigrateSectorSize)
		//fmt.Println("1111: ", v.ParallelMigrateSectorSize)
		//fmt.Println("1111: ", v.MaxStoreSectorSize)
		//fmt.Println("1111: ", v.StoreSectorSize)
		//fmt.Println("1111: ", v.Status)
		//fmt.Println("1111: ", v.CreatedAt)
		//fmt.Println("1111: ", v.UpdatedAt)
		//fmt.Println("1111: ", v.FtpEnv.FtpPort)
		//fmt.Println("1111: ", v.FtpEnv.FtpUser)
		//fmt.Println("1111: ", v.FtpEnv.FtpPassword)
	}
	for groupid, ss := range storeMachinesMap {
		smhMap[groupid] = NewCycleQueue(len(ss) + 1)
		for _, sm := range ss {
			smhMap[groupid].Push(sm)
		}
	}
	if len(smhMap) == 0 {
		return xerrors.New("no machine can store")
	}
	smh.Handler = smhMap
	return nil
}

func (smh *StoreMachineHandle) SelectStoreMachine(ctx context.Context, kind, workerip string) (m *StoreMachines, err error) {
	if kind == NetWorkIOBalance {
		smh.MutexLock.Lock()
		defer smh.MutexLock.Unlock()
		//machine, err := GetMachineByIP(ctx, workerip, os.Getenv("SUBMINERID"))
		//if err != nil {
		//	return m, xerrors.Errorf("[%s]cann't found machine by ip. err:%v", workerip, err)
		//}
		//groupid := machine.StoreGroupID
		groupid := "1"
		if _, ok := smh.Handler[groupid]; !ok {
			return m, xerrors.Errorf("[%s] machine store group id not same as store machine group id", workerip)
		}
		for {
			if smh.Handler[groupid].IsEmpty() {
				return nil, xerrors.Errorf("[%s] no store machine can store ", workerip)
			}
			Data := smh.Handler[groupid].Pop()
			if Data == nil {
				continue
			}
			m = Data.(*StoreMachines)
			if err := StoreMachineApply(ctx, MongoHandler.Collection("machines"), *m); err != nil {
				log.Error("StoreMachineApply err ", err)
				continue
			}
			log.Infof("StoreMachineApply StoreIP[%s] ParallelMigrateSectorSize[%d] max[%d] ", m.StoreIP, m.ParallelMigrateSectorSize+1, m.MaxParallelMigrateSectorSize)
			m.ParallelMigrateSectorSize++
			m.StoreSectorSize++
			smh.Handler[groupid].Push(m)
			break
		}
	}
	return m, nil
}

func (smh *StoreMachineHandle) DoneStoreMachine(ctx context.Context, task *MigrateTasks) error {
	smh.MutexLock.Lock()
	defer smh.MutexLock.Unlock()
	sm, err := GetStoreMachineByStoreIp(ctx, MongoHandler.Collection("machines"), StoreMachines{StoreIP: task.StoreIP, StorePath: task.StorePath})
	if err != nil {
		return err
	}
	out := &StoreMachines{}
	if *sm == *out {
		return nil
	}
	if smh.Handler[sm.GroupID] == nil {
		return nil
	}
	index := -1
	for i := 0; i < smh.Handler[sm.GroupID].Size(); i++ {
		if smh.Handler[sm.GroupID].DataArray()[i] == nil {
			continue
		}
		if smh.Handler[sm.GroupID].DataArray()[i].(*StoreMachines).StoreIP == task.StoreIP &&
			smh.Handler[sm.GroupID].DataArray()[i].(*StoreMachines).StorePath == task.StorePath {
			index = i
		}
	}
	log.Errorf("StoreMachineUseDone StoreIP[%s] ParallelMigrateSectorSize[%d] max[%d] ", sm.StoreIP, sm.ParallelMigrateSectorSize-1, sm.MaxParallelMigrateSectorSize)
	if err := StoreMachineUseDone(ctx, MongoHandler.Collection("machines"), *sm); err != nil {
		return err
	}
	sm.ParallelMigrateSectorSize--
	if index == -1 {
		smh.Handler[sm.GroupID].Push(sm)
	} else {
		smh.Handler[sm.GroupID].IndexDataChange(index, sm)
	}
	return nil
}

func (smh *StoreMachineHandle) CancelStoreMachine(ctx context.Context, task *MigrateTasks) error {
	smh.MutexLock.Lock()
	defer smh.MutexLock.Unlock()
	sm, err := GetStoreMachineByStoreIp(ctx, MongoHandler.Collection("machines"), StoreMachines{StoreIP: task.StoreIP, StorePath: task.StorePath})
	if err != nil {
		return err
	}
	out := &StoreMachines{}
	if *sm == *out {
		return nil
	}
	if smh.Handler[sm.GroupID] == nil {
		return nil
	}
	index := -1
	for i := 0; i < smh.Handler[sm.GroupID].Size(); i++ {
		if smh.Handler[sm.GroupID].DataArray()[i] == nil {
			continue
		}
		if smh.Handler[sm.GroupID].DataArray()[i].(*StoreMachines).StoreIP == task.StoreIP &&
			smh.Handler[sm.GroupID].DataArray()[i].(*StoreMachines).StorePath == task.StorePath {
			index = i
		}
	}
	sm.Status = 1
	log.Errorf("StoreMachineUseFailed StoreIP[%s] ParallelMigrateSectorSize[%d] max[%d] ", sm.StoreIP, sm.ParallelMigrateSectorSize-1, sm.MaxParallelMigrateSectorSize)
	if err := StoreMachineUseFailed(ctx, MongoHandler.Collection("machines"), *sm); err != nil {
		return err
	}
	sm.ParallelMigrateSectorSize--
	sm.StoreSectorSize--
	if index == -1 {
		smh.Handler[sm.GroupID].Push(sm)
	} else {
		smh.Handler[sm.GroupID].IndexDataChange(index, sm)
	}
	return nil
}

func init() {
	MongoHandler = InitMongo("mongodb://124.220.208.74:27017", "lotus", 10*time.Second, 100)
	//MongoHandler = InitMongo("mongodb://192.168.0.11:27017", "test", 10*time.Second, 100)
}

func InitMongo(uri, name string, timeout time.Duration, num uint64) *mongo.Database {
	_, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	//o := options.Client().ApplyURI(uri)
	//o := options.Client().ApplyURI("mongodb://root:123456@124.220.208.74:27017/lotus?authSource=admin")
	//o.SetMaxPoolSize(num)
	//client, err := mongo.Connect(ctx, o)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri).SetConnectTimeout(5*time.Second))
	if err != nil {
		panic(err)
	}
	return client.Database(name)
}

func MonitorStoreMachine() {
	//if MongoHandler == nil {
	//	Init()
	//	log.Infof("init DB\n")
	//}
	f := bson.D{{"Status", 1}, {"role", "storage"}, {"ip", bson.D{{"$ne", "127.0.0.1"}}}}
	oldCount := 0
	log.Infof("oldCount:[%d]\n", oldCount)
	for {
		newCount, err := MongoHandler.Collection("machines").CountDocuments(context.TODO(), f)
		//newCount, err := MongoHandler.Collection("machines").CountDocuments(context.TODO(), bson.D{})
		if err != nil {
			log.Error("StoreMachines().CountDocuments err :", err)
			continue
		}
		log.Infof("store machine count :[%d] -> [%d]\n", oldCount, int(newCount))
		if oldCount != int(newCount) {
			log.Infof("store machine count change:[%d] -> [%d]\n", oldCount, int(newCount))
			if err := StoreMachineManager.NewStoreMachines(context.Background()); err != nil {
				log.Error("StoreMachineManager.NewStoreMachines err :", err)
				time.Sleep(10 * time.Minute)
				continue
			}
			oldCount = int(newCount)
		}
		time.Sleep(10 * time.Minute)
	}
}
