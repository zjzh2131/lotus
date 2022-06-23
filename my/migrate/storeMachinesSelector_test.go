package migration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	//"github.com/filecoin-project/lotus/extern/sector-storage/config"
	//"github.com/filecoin-project/lotus/extern/sector-storage/service/mod"
)

func TestNewStoreMachines(t *testing.T) {
	//_ = config.Init()
	ctx := context.Background()
	if err := NewStoreMachines(ctx); err != nil {
		fmt.Println("NewStoreMachines err", err)
		return
	}
	for k, v := range StoreMachineManager().Handler {
		fmt.Printf("groupid:[%s]\t[queue]:[%s] \n", k, v.DataArray())
	}
}

func MockMigrateBegin() (*mongo.InsertOneResult, error) {
	task := MigrateTasks{
		SectorID:     "s-t01000-0",
		Version:      "",
		FromIP:       "192.168.139.128",
		FromPath:     "/home/hp/.genesis-sectors",
		StoreIP:      "192.168.0.11",
		StorePath:    "/home/zjzh/migrate",
		FailureCount: 0,
		FtpStatus:    0,
		StartTime:    time.Now().Unix(),
		Error:        "",
		TaskType:     MigrateType_Sealed,
	}
	insertResult, err := AddMigrateTask(context.TODO(), &task)
	if err != nil {
		log.Error("AddMigrateTask error")
		return nil, err
	}

	//objID := insertResult.InsertedID.(primitive.ObjectID)
	return insertResult, nil

}

func MockMigrateFinish(objId primitive.ObjectID) {
	task := MigrateTasks{
		Error: "success",
	}
	UpdateMigrateTaskByID(context.TODO(), objId, &task)
}

func TestSelectStoreMachine(t *testing.T) {
	//因 CountMigrateTask 导致无法单独测出 并发数量的限制正常，需整体测，其它情形可以测。
	//_ = config.Init()
	ctx := context.Background()
	if err := NewStoreMachines(ctx); err != nil {
		fmt.Println("NewStoreMachines err", err)
		return
	}
	w := sync.WaitGroup{}
	workerip := "172.18.0.1"
	for i := 0; i < 66; i++ {
		w.Add(1)
		go func(i int) {
			//rand := i % 5
			rand := 2
			time.Sleep(time.Duration(rand) * time.Second)

			insertResult, err := MockMigrateBegin()
			objID := insertResult.InsertedID.(primitive.ObjectID)
			defer MockMigrateFinish(objID)

			sm, err := SelectStoreMachine(ctx, NetWorkIOBalance, workerip)
			if sm == nil {
				if err != nil {
					fmt.Println("SelectStoreMachine err ", err)
					w.Done()
					return
				}
			}
			fmt.Printf("i[%d]\trand[%d]\tstoreip[%s]\tstorepath[%s]\tcurrent:[%d]\tlimit[%d\tStoreSectorSize:[%d]\tMaxStoreSectorSize:[%d]\n",
				i, rand, sm.StoreIP, sm.StorePath, sm.ParallelMigrateSectorSize, sm.MaxParallelMigrateSectorSize, sm.StoreSectorSize, sm.MaxStoreSectorSize)

			if rand == 3 {
				if err := CancelStoreMachine(ctx, &MigrateTasks{StorePath: sm.StorePath, StoreIP: sm.StoreIP}); err != nil {
					fmt.Println("CancelStoreMachine err ", err)
				}
			} else {
				if err := DoneStoreMachine(ctx, &MigrateTasks{StorePath: sm.StorePath, StoreIP: sm.StoreIP}); err != nil {
					fmt.Println("DoneStoreMachine err ", err)
				}
			}
			w.Done()
		}(i)
	}
	w.Wait()
}
