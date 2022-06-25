package migration

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	//"github.com/filecoin-project/lotus/extern/sector-storage/config"
	//"github.com/filecoin-project/lotus/extern/sector-storage/service/db"
	//"github.com/filecoin-project/lotus/extern/sector-storage/service/mod"
)

//func AddMigrateTaskLog(ctx context.Context, task *mod.TaskLog) error {
//	return db.AddMigrateTaskLog(ctx, config.TaskLog(), task)
//}
//
//func UpdateMigrateTaskLog(ctx context.Context, task *mod.TaskLog) error {
//	return db.UpdateMigrateTaskLog(ctx, config.TaskLog(), task)
//}

func AddMigrateTask(ctx context.Context, task *MigrateTasks) (*mongo.InsertOneResult, error) {
	//return db.AddMigrateTask(ctx, config.MigrateTask(), task)
	insertResult, err := MongoHandler.Collection("MigrateTasks").InsertOne(ctx, task)
	if err != nil {
		return nil, err
	}
	return insertResult, nil
}

//func RemoveMigrateTask(ctx context.Context, task *MigrateTasks) error {
//	return db.RemoveMigrateTask(ctx, config.MigrateTask(), task)
//}

func UpdateMigrateTaskByID(ctx context.Context, objId primitive.ObjectID, task *MigrateTasks) error {
	//return db.UpdateMigrateTask(ctx, config.MigrateTask(), task)
	//f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}, {"status", ""}}
	update := bson.D{{"$set", bson.D{{"Error", task.Error}}}}
	result, err := MongoHandler.Collection("MigrateTasks").UpdateByID(ctx, objId, update)
	if err != nil {
		log.Error(err)
		return err
	}

	if result.MatchedCount != 0 {
		fmt.Println("matched and replaced an existing document")

	}
	return nil
}

//func UpdateMigrateTaskFailurecount(ctx context.Context, task *MigrateTasks) error {
//	return db.UpdateMigrateTaskFailurecount(ctx, config.MigrateTask(), task)
//}
//
//func DeleteMigrateTask(ctx context.Context, task *mod.MigrateTasks) error {
//	return db.DeleteMigrateTask(ctx, config.MigrateTask(), task)
//}
//
//func GetMigrateTasks(ctx context.Context, request mod.MigrateRequest) ([]*mod.MigrateTasks, error) {
//	return db.GetMigrateTasks(ctx, config.MigrateTask(), request)
//}
//
//func GetDeleteMigrateTask(ctx context.Context, task mod.MigrateTasks) (*mod.MigrateTasks, error) {
//	return db.GetDeleteMigrateTask(ctx, config.MigrateTask(), task)
//}
//
//func CheckMigrateTaskExsit(ctx context.Context, task *mod.MigrateTasks) error {
//	return db.CheckMigrateTaskExsit(ctx, config.MigrateTask(), task)
//}
//
//func UpdateExsitMigrateTask(ctx context.Context, task *mod.MigrateTasks) error {
//	return db.UpdateExsitMigrateTask(ctx, config.MigrateTask(), task)
//}
//
//func GetMigrateTasksByNodeIDs(ctx context.Context, nodeIDs []string) ([]*mod.MigrateTasks, error) {
//	return db.GetMigrateTasksByNodeIDs(ctx, config.MigrateTask(), nodeIDs)
//}
//
//func GetMigrateTasksByNumber(ctx context.Context, number uint64) (*mod.MigrateTasks, error) {
//	return db.GetMigrateTasksByNumber(ctx, config.MigrateTask(), number)
//}
