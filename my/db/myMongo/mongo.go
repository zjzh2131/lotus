package myMongo

import (
	"context"
	"fmt"
	"github.com/filecoin-project/lotus/my/myModel"
	"github.com/filecoin-project/lotus/my/myUtils"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	MongoHandler *mongo.Database
)

const (
	SealingTasks    = "sealing_tasks"
	SealingTaskLogs = "sealing_task_logs"
	Sectors         = "sectors"
	Machines        = "machines"
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

func FindByStatus(taskStatus string) ([]*myModel.SealingTask, error) {
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"task_status": taskStatus},
		},
	}
	tasks, err := findTasks(filter)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func FindBySIdTypeStatus(sId uint64, taskType, taskStatus string) ([]*myModel.SealingTask, error) {
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"sector_ref.id.number": sId},
			bson.M{"task_type": taskType},
			bson.M{"task_status": taskStatus},
		},
	}
	tasks, err := findTasks(filter)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func FindBySIdStatus(sId uint64, taskStatus string) ([]*myModel.SealingTask, error) {
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"sector_ref.id.number": sId},
			bson.M{"task_status": taskStatus},
		},
	}
	tasks, err := findTasks(filter)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func findTasks(filter bson.M) ([]*myModel.SealingTask, error) {
	var tasks []*myModel.SealingTask

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

func FindByObjId(objId string) (*myModel.SealingTask, error) {
	hex, err := primitive.ObjectIDFromHex(objId)
	if err != nil {
		return nil, err
	}
	filter := bson.M{"_id": hex}
	tasks, err := findTasks(filter)
	if err != nil {
		return nil, err
	}
	if len(tasks) == 0 {
		return nil, err
	}
	return tasks[0], nil
}

func updateById(collectionName string, objId primitive.ObjectID, update bson.M) error {
	_, err := MongoHandler.Collection(collectionName).UpdateByID(context.TODO(), objId, update)
	if err != nil {
		return err
	}
	return nil
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

func UpdateError(objId primitive.ObjectID, errStr string) error {
	update := bson.M{}
	update["$set"] = bson.M{"task_error": errStr}
	_, err := MongoHandler.Collection("sealing_tasks").UpdateByID(context.TODO(), objId, update)
	if err != nil {
		return err
	}
	return nil
}

func UpdateTaskResStatus(objId primitive.ObjectID, state, res string) error {
	update := bson.M{}
	update["$set"] = bson.D{
		bson.E{Key: "task_result", Value: res},
		bson.E{Key: "task_status", Value: state},
	}
	_, err := MongoHandler.Collection(SealingTasks).UpdateByID(context.TODO(), objId, update)
	if err != nil {
		return err
	}
	return nil
}

func InitTask(sector storage.SectorRef, taskType string, taskStatus string, taskParameters ...interface{}) error {
	task := myModel.SealingTask{
		SectorRef:      sector,
		TaskParameters: []string{},
		TaskType:       taskType,
		TaskError:      "",
		TaskResult:     "",
		TaskStatus:     taskStatus,

		WorkerIp:   "",
		WorkerPath: "",

		StartTime: 0,
		EndTime:   0,

		NodeId:    "",
		ClusterId: "",

		CreatedAt: time.Now().UnixMilli(),
		CreatedBy: "t0" + strconv.Itoa(int(sector.ID.Miner)),
		UpdatedAt: 0,
		UpdatedBy: "",
	}
	for i, tp := range taskParameters {
		fmt.Println(i, tp)
		tpStr, err := myUtils.Interface2Json(tp)
		if err != nil {
			return err
		}
		fmt.Println(tpStr)
		task.TaskParameters = append(task.TaskParameters, tpStr)
	}
	_, err := Insert("sealing_tasks", &task)
	if err != nil {
		return err
	}
	return nil
}

func InitSector(sector storage.SectorRef, sectorType string, sectorStatus string) error {
	s := myModel.Sector{
		SectorId:     uint64(sector.ID.Number),
		SectorStatus: sectorStatus,
		SectorType:   sectorType,

		NodeId:    "",
		ClusterId: "",

		CreatedAt: time.Now().UnixMilli(),
		CreatedBy: "",
		UpdatedAt: 0,
		UpdatedBy: "",
	}
	_, err := Insert("sectors", &s)
	if err != nil {
		return err
	}
	return nil
}

func InsertTaskLog(task *myModel.SealingTask, workerIp string) (string, error) {
	tl := myModel.SealingTaskLog{
		SectorRef:      task.SectorRef,
		WorkerIp:       workerIp,
		WorkerPath:     "",
		StartTime:      time.Now().UnixMilli(),
		EndTime:        0,
		TaskParameters: nil,
		TaskType:       task.TaskType,
		TaskError:      task.TaskError,
		TaskStatus:     task.TaskType,

		NodeId:    "",
		ClusterId: "",

		CreatedAt: time.Now().UnixMilli(),
		CreatedBy: workerIp,
		UpdatedAt: 0,
		UpdatedBy: "",
	}
	insertResult, err := Insert("sealing_task_logs", &tl)
	id := fmt.Sprintf("%v", insertResult.InsertedID)
	id = strings.Split(id, `"`)[1]
	if err != nil {
		return "", err
	}
	return id, nil
}

func UpdateTaskLog(objId string, update bson.M) error {
	hex, err := primitive.ObjectIDFromHex(objId)
	if err != nil {
		return err
	}
	err = updateById(SealingTaskLogs, hex, update)
	if err != nil {
		return err
	}
	return nil
}

func Transaction(transFunc func() error) error {
	err := MongoHandler.Client().UseSession(context.TODO(), func(sessionContext mongo.SessionContext) error {
		var err error
		err = sessionContext.StartTransaction()
		if err != nil {
			return err
		}

		err = transFunc()
		if err != nil {
			sessionContext.AbortTransaction(sessionContext)
			return err
		}

		sessionContext.CommitTransaction(sessionContext)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func FindSectorsBySid(sid uint64) (*myModel.Sector, error) {
	var out myModel.Sector
	var err error

	filter := bson.M{"sector_id": sid}
	singleResult := MongoHandler.Collection(Sectors).FindOne(context.TODO(), filter)
	if err = singleResult.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}

	if err = singleResult.Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func FindSmallerSectorId(sids []uint64, sidCap uint64, taskStatus string) ([]uint64, error) {
	pipeline := mongo.Pipeline{
		bson.D{
			{"$match", bson.D{
				{"sector_ref.id.number", bson.M{"$nin": sids}},
				{"task_status", taskStatus},
			}},
		},
		bson.D{
			{"$group", bson.D{
				{"_id", "$sector_ref.id.number"},
			}},
		},
		bson.D{
			{"$sort", bson.M{"_id": 1}},
		},
		bson.D{
			{"$limit", sidCap},
		},
		bson.D{
			{"$project", bson.D{
				{"_id", 1},
			}},
		},
	}

	opt := &options.AggregateOptions{}
	findResults, err := MongoHandler.Collection(SealingTasks).Aggregate(context.TODO(), pipeline, opt)
	if err != nil {
		return nil, err
	}
	defer findResults.Close(context.TODO())

	var out []uint64
	for findResults.Next(context.TODO()) {
		var sid sid
		//var m map[string]interface{}
		err := findResults.Decode(&sid)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		out = append(out, sid.ID)
	}
	return out, nil
}

type sid struct {
	ID uint64 `json:"id" bson:"_id,omitempty"` // ObjectId
}

func FindBySid() {
	//var out myModel.Sector
	//var err error
	//
	//filter := bson.M{"sector_id": sid}
	//singleResult := MongoHandler.Collection(SealingTasks).FindOne(context.TODO(), filter)
	//if err = singleResult.Err(); err != nil {
	//	if err == mongo.ErrNoDocuments {
	//		return nil, nil
	//	}
	//	return nil, err
	//}
	//
	//if err = singleResult.Decode(&out); err != nil {
	//	return nil, err
	//}
	//return &out, nil
}

func FindAndModifyForStatus(objId primitive.ObjectID, oldTaskStatus, newTaskStatus string) (ok bool, err error) {
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"_id": objId},
			//bson.M{"task_type": ""},
			bson.M{"task_status": oldTaskStatus},
		},
	}
	updater := bson.M{}
	updater["$set"] = bson.D{
		bson.E{Key: "task_status", Value: newTaskStatus},
	}
	singleResult := MongoHandler.Collection(SealingTasks).FindOneAndUpdate(context.TODO(), filter, updater)
	if err = singleResult.Err(); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func FindTaskByWorkerIp(workerIp string) ([]*myModel.SealingTask, error) {
	filter := bson.M{
		"$and": []interface{}{
			bson.M{"worker_ip": workerIp},
		},
	}
	tasks, err := findTasks(filter)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}
