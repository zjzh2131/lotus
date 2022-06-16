package myModel

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

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

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type SealingTaskLog struct {
	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId

	SectorRef storage.SectorRef `json:"sector_ref" bson:"sector_ref"`

	WorkerIp   string `json:"worker_ip" bson:"worker_ip"`
	WorkerPath string `json:"worker_path" bson:"worker_path"`

	StartTime int64 `json:"start_time" bson:"start_time"`
	EndTime   int64 `json:"end_time" bson:"end_time"`

	TaskParameters []string `json:"task_parameters" bson:"task_parameters"`
	TaskType       string   `json:"task_type" bson:"task_type"`
	TaskError      string   `json:"task_error" bson:"task_error"`
	TaskStatus     string   `json:"task_status" bson:"task_status"`

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type Sector struct {
	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId

	SectorId     uint64 `json:"sector_id" bson:"sector_id"`
	SectorStatus string `json:"sector_status" bson:"sector_status"`
	SectorType   string `json:"sector_type" bson:"sector_type"`

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type Machine struct {
	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId

	Ip           string       `json:"ip" bson:"ip"`
	HardwareInfo hardwareInfo `json:"hardware_info" bson:"hardware_info"`

	Path string `json:"path" bson:"path"`
	Role string `json:"role" bson:"role"` // lotus/miner/winPost/wdPost/worker/storage

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type hardwareInfo struct {
}

type APParam0 []abi.UnpaddedPieceSize

type APParam1 abi.UnpaddedPieceSize

type MyFinalizeSectorOut struct{}
