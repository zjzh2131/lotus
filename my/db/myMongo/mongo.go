package myMongo

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/my/myModel"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	MongoHandler *mongo.Database
)

func init() {
	MongoHandler = InitMongo("mongodb://124.220.208.74:27017", "lotus", 10*time.Second, 100)
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

func Insert(collectionName string, content interface{}) (*mongo.InsertOneResult, error) {
	insertResult, err := MongoHandler.Collection(collectionName).InsertOne(context.TODO(), content)
	if err != nil {
		return nil, err
	}
	return insertResult, nil
}

func FindTasks(taskStatue string) ([]*myModel.SealingTask, error) {
	var tasks []*myModel.SealingTask
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"task_status": taskStatue},
		},
	}
	var findOptions *options.FindOptions
	findOptions = &options.FindOptions{}
	// 排序
	sortM := map[string]interface{}{}
	sortM["created_at"] = -1
	findOptions.Sort = sortM
	// 分页
	//findOptions.SetLimit(pageSize)
	//findOptions.SetSkip((pageNum - 1) * pageSize)
	findResults, err := MongoHandler.Collection("sealing_tasks").Find(context.TODO(), filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := findResults.Close(context.TODO())
		if err != nil {
			fmt.Println(err)
		}
	}()
	for findResults.Next(context.TODO()) {
		var task myModel.SealingTask
		err := findResults.Decode(&task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &task)
	}
	return tasks, nil
}

func UpdateStatus(objId primitive.ObjectID, state string) error {
	update := bson.M{}
	update["$set"] = bson.M{"task_status": state}
	_, err := MongoHandler.Collection("sealing_tasks").UpdateByID(context.TODO(), objId, update)
	if err != nil {
		return err
	}
	return nil
}

func UpdateResult(objId primitive.ObjectID, result string) error {
	update := bson.M{}
	update["$set"] = bson.M{"task_result": result}
	_, err := MongoHandler.Collection("sealing_tasks").UpdateByID(context.TODO(), objId, update)
	if err != nil {
		return err
	}
	return nil
}
