package migration

import (
	"context"
	"time"

	//"github.com/filecoin-project/lotus/extern/sector-storage/service/mod"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/xerrors"
)

func GetStoreMachineByStoreIp(ctx context.Context, c *mongo.Collection, m StoreMachines) (*StoreMachines, error) {
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	rs := c.FindOne(ctx, f)
	out := &StoreMachines{}
	if err := rs.Decode(out); err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}
	return out, nil
}

func StoreMachineUseDone(ctx context.Context, c *mongo.Collection, m StoreMachines) error {
	//opts := options.Update().SetUpsert(true)
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	u := bson.D{
		{"$set", bson.D{
			{"parallelmigratesectorsize", m.ParallelMigrateSectorSize - 1},
			{"updatedat", time.Now()},
		}}}
	_, err := c.UpdateOne(ctx, f, u)
	return err
}

func StoreMachineUseFailed(ctx context.Context, c *mongo.Collection, m StoreMachines) error {
	//opts := options.Update().SetUpsert(true)
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	u := bson.D{
		{"$set", bson.D{
			{"storesectorsize", m.StoreSectorSize - 1},
			{"parallelmigratesectorsize", m.ParallelMigrateSectorSize - 1},
			{"updatedat", time.Now()},
			//{"status", m.Status},
		}}}
	_, err := c.UpdateOne(ctx, f, u)
	return err
}

func StoreMachineDisable(ctx context.Context, c *mongo.Collection, m StoreMachines) error {
	//opts := options.Update().SetUpsert(true)
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	u := bson.D{
		{"$set", bson.D{
			{"status", 0},
			{"updatedat", time.Now()},
		}}}
	_, err := c.UpdateOne(ctx, f, u)
	return err
}

func StoreMachineUseable(ctx context.Context, c *mongo.Collection, m StoreMachines) error {
	//opts := options.Update().SetUpsert(true)
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	u := bson.D{
		{"$set", bson.D{
			{"status", 1},
			{"updatedat", time.Now()},
		}}}
	_, err := c.UpdateOne(ctx, f, u)
	return err
}

func StoreMachineApply(ctx context.Context, c *mongo.Collection, m StoreMachines) error {
	if m.ParallelMigrateSectorSize >= m.MaxParallelMigrateSectorSize {
		return xerrors.New("current resource allocation is full")
	}
	if m.StoreSectorSize >= m.MaxStoreSectorSize {
		if m.ParallelMigrateSectorSize == 0 {
			_ = StoreMachineDisable(ctx, c, m)
		}
		return xerrors.New("total resource allocation is full")
	}
	//crs, err := CountMigrateTask(ctx, m)
	//if err != nil {
	//	return err
	//}
	//if m.ParallelMigrateSectorSize != int32(crs) {
	//	fmt.Printf("---------ParallelMigrateSectorSize: [%d], tasks num: [%d]\n", m.ParallelMigrateSectorSize, int32(crs))
	//}
	//opts := options.Update().SetUpsert(true)
	f := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}}
	u := bson.D{
		{"$set", bson.D{
			{"parallelmigratesectorsize", int32(m.ParallelMigrateSectorSize + 1)},
			{"storesectorsize", m.StoreSectorSize + 1},
			{"updatedat", time.Now()},
		}}}
	_, err := c.UpdateOne(ctx, f, u)
	return err
}

func CountMigrateTask(ctx context.Context, m StoreMachines) (int32, error) {
	countf := bson.D{{"storeip", m.StoreIP}, {"storepath", m.StorePath}, {"Error", ""}}
	//crs, err := config.MigrateTask().CountDocuments(ctx, countf)
	crs, err := MongoHandler.Collection("MigrateTasks").CountDocuments(ctx, countf)
	if err != nil {
		return 0, err
	}
	return int32(crs), nil
}
