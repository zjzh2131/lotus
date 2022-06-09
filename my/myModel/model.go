package myModel

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var MyCtx *cli.Context

//type SealingTask struct {
//	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId
//
//	SectorId uint64 `json:"sector_id" bson:"sector_id"`
//
//	MinerId uint64 `json:"miner_id" bson:"miner_id"`
//	MAddr   uint64 `json:"m_addr" bson:"m_addr"`
//
//	PATH storiface.SectorPaths `json:"path" bson:"path"`
//
//	WorkerIp   string `json:"worker_ip" bson:"worker_ip"`
//	WorkerPath string `json:"worker_path" bson:"worker_path"`
//	StartTime  int64  `json:"start_time" bson:"start_time"`
//	EndTime    int64  `json:"end_time" bson:"end_time"`
//
//	TaskParameters []string `json:"task_parameters" bson:"task_parameters"`
//	TaskType       string   `json:"task_type" bson:"task_type"`
//	TaskError      string   `json:"task_error" bson:"task_error"`
//	TaskResult     string   `json:"task_result" bson:"task_result"`
//	TaskStatus     string   `json:"task_status" bson:"task_status"`
//	TaskTime       int64    `json:"task_time" bson:"task_time"`
//
//	NodeId    int64 `json:"node_id" bson:"node_id"`
//	ClusterId int64 `json:"cluster_id" bson:"cluster_id"`
//
//	CreatedAt int64  `json:"created_at" bson:"created_at"`
//	CreatedBy string `json:"created_by" bson:"created_by"`
//	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
//	UpdatedBy string `json:"updated_by" bson:"updated_by"`
//}

type SealingTask struct {
	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId

	SectorRef storage.SectorRef `json:"sector_ref" bson:"sector_ref"`

	WorkerIp   string `json:"worker_ip" bson:"worker_ip"`
	WorkerPath string `json:"worker_path" bson:"worker_path"`
	StartTime  int64  `json:"start_time" bson:"start_time"`
	EndTime    int64  `json:"end_time" bson:"end_time"`

	TaskParameters []string `json:"task_parameters" bson:"task_parameters"`
	TaskType       string   `json:"task_type" bson:"task_type"`
	TaskError      string   `json:"task_error" bson:"task_error"`
	TaskResult     string   `json:"task_result" bson:"task_result"`
	TaskStatus     string   `json:"task_status" bson:"task_status"`
	TaskTime       int64    `json:"task_time" bson:"task_time"`

	NodeId    int64 `json:"node_id" bson:"node_id"`
	ClusterId int64 `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type APParam0 []abi.UnpaddedPieceSize

type APParam1 abi.UnpaddedPieceSize

type MyFinalizeSectorOut struct{}
