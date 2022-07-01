package myModel

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/specs-storage/storage"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var DelTaskInfo = map[sealtasks.TaskType][]string{
	sealtasks.TTAddPiece: []string{string(sealtasks.TTAddPiece), string(sealtasks.TTPreCommit1), string(sealtasks.TTPreCommit2),
		string(sealtasks.TTCommit1), string(sealtasks.TTCommit2), string(sealtasks.TTFinalize)},

	sealtasks.TTPreCommit1: []string{string(sealtasks.TTPreCommit1), string(sealtasks.TTPreCommit2), string(sealtasks.TTCommit1),
		string(sealtasks.TTCommit2), string(sealtasks.TTFinalize)},

	sealtasks.TTPreCommit2: []string{string(sealtasks.TTPreCommit2), string(sealtasks.TTCommit1), string(sealtasks.TTCommit2),
		string(sealtasks.TTFinalize)},

	sealtasks.TTCommit1: []string{string(sealtasks.TTCommit1), string(sealtasks.TTCommit2), string(sealtasks.TTFinalize)},

	sealtasks.TTCommit2: []string{string(sealtasks.TTCommit2), string(sealtasks.TTFinalize)},

	sealtasks.TTFinalize: []string{string(sealtasks.TTFinalize)},
}

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
	TaskPath       string   `json:"task_path" bson:"task_path"`

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

	WorkerIp string `json:"worker_ip" bson:"worker_ip"`

	SectorRef    storage.SectorRef `json:"sector_ref" bson:"sector_ref"`
	SectorId     uint64            `json:"sector_id" bson:"sector_id"`
	SectorStatus string            `json:"sector_status" bson:"sector_status"`
	SectorType   string            `json:"sector_type" bson:"sector_type"` // cc

	StorageIp   string `json:"storage_ip" bson:"storage_ip"`
	StoragePath string `json:"storage_path" bson:"storage_path"`
	MigratePath string `json:"migrate_path" bson:"migrate_path"`

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type Machine struct {
	ID primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId

	Ip           string        `json:"ip" bson:"ip"`
	HardwareInfo *hardwareInfo `json:"hardware_info" bson:"hardware_info"`
	ControlInfo  *controlInfo  `json:"control_info" bson:"control_info"`

	Role string `json:"role" bson:"role"` // lotus/miner/winPost/wdPost/worker/storage

	WorkerLocalPath string `json:"worker_local_storage_path" bson:"worker_local_storage_path"` // worker
	WorkerMountPath string `json:"worker_mount_path" bson:"worker_mount_path"`                 // worker
	MinerLocalPath  string `json:"miner_local_storage_path" bson:"miner_local_storage_path"`   // miner
	MinerMountPath  string `json:"miner_mount_path" bson:"miner_mount_path"`                   // miner
	StoragePath     string `json:"storage_path" bson:"storage_path"`                           // storage

	NodeId    string `json:"node_id" bson:"node_id"`
	ClusterId string `json:"cluster_id" bson:"cluster_id"`

	MaxParallelMigrateSectorSize uint64 `json:"max_parallel_migrate_sector_size" bson:"MaxParallelMigrateSectorSize"`
	Parallelmigratesectorsize    uint64 `json:"parallelmigratesectorsize" bson:"parallelmigratesectorsize"`
	MaxStoreSectorSize           uint64 `json:"max_store_sector_size" bson:"MaxStoreSectorSize"`
	Storesectorsize              uint64 `json:"storesectorsize" bson:"storesectorsize"`
	Status                       int64  `json:"status" bson:"Status"`
	FtpEnv                       FtpEnv `json:"ftp_env" bson:"FtpEnv"`

	CreatedAt int64  `json:"created_at" bson:"created_at"`
	CreatedBy string `json:"created_by" bson:"created_by"`
	UpdatedAt int64  `json:"updated_at" bson:"updated_at"`
	UpdatedBy string `json:"updated_by" bson:"updated_by"`
}

type hardwareInfo struct {
}

type controlInfo struct {
}

type FtpEnv struct {
	FtpPort     string `json:"FtpPort" bson:"FtpPort"`
	FtpUser     string `json:"FtpUser" bson:"FtpUser"`
	FtpPassword string `json:"FtpPassword" bson:"FtpPassword"`
}

type APParam0 []abi.UnpaddedPieceSize

type APParam1 abi.UnpaddedPieceSize

type MyFinalizeSectorOut struct{}
