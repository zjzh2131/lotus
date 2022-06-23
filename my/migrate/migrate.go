package migration

import (
	"github.com/filecoin-project/go-state-types/abi"
	"go.mongodb.org/mongo-driver/bson/primitive"

	//"github.com/filecoin-project/lotus/extern/sector-storage/service/mod"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	//"github.com/filecoin-project/lotus/extern/sector-storage/utils/file"
	//"github.com/filecoin-project/lotus/extern/sector-storage/utils/ftp"
	//"github.com/filecoin-project/lotus/node/impl"
	"golang.org/x/xerrors"
	//"net/http"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("migration")

const (
	ConnectTimeout    = 30
	ReConnectTime     = 10
	MigrateFinishStat = "MigrateFinishStat"
)

type _TaskType int

const (
	MigrateType_Sealed _TaskType = iota
	MigrateType_Updated
)

type MigrateParam struct {
	SectorID  string
	FromIP    string
	FromPath  string
	StoreIP   string
	StorePath string
	FtpEnv    FtpEnvStruct
}

type MigrateTasks struct {
	ID           primitive.ObjectID `json:"id" bson:"_id,omitempty"` // ObjectId
	SectorID     string             `json:"SectorID" bson:"SectorID"`
	Version      string             `json:"Version" bson:"Version"`
	FromIP       string             `json:"FromIP" bson:"FromIP"`
	FromPath     string             `json:"FromPath" bson:"FromPath"`
	StoreIP      string             `json:"storeip" bson:"storeip"`
	StorePath    string             `json:"storepath" bson:"storepath"`
	FailureCount int32              `json:"FailureCount" bson:"FailureCount"`
	FtpStatus    int                `json:"FtpStatus" bson:"FtpStatus"`
	StartTime    int64              `json:"StartTime" bson:"StartTime"`
	Error        string             `json:"Error" bson:"Error"`
	TaskType     _TaskType          `json:"TaskType" bson:"TaskType"`
}

type MigrateRequest struct {
	Success      bool
	SectorID     string
	Version      string
	FromIP       string
	FromPath     string
	StoreIP      string
	StorePath    string
	FailureCount int32
	StartTime    int64
	EndTime      int64
	CostTime     int64
	FtpStatus    int
	Error        string
	TaskType     _TaskType
}

const mutexLocked = 1

func MutexLocked(m *sync.Mutex) bool {
	state := reflect.ValueOf(m).Elem().FieldByName("state")
	return state.Int()&mutexLocked == mutexLocked
}

func createMigrateFinishStateFile(fname string) error {
	if err := WriteDataToFile(fname, []byte("")); err != nil {
		log.Errorf("write c1out file failed %v", err)
		return err
	}
	return nil
}

func createMigrateFinishStateFileWithFTP(f *FtpClient, src, des string) error {
	if err := WriteDataToFile(src, []byte("")); err != nil {
		log.Errorf("write c1out file failed %v", err)
		return err
	}
	if err := f.UploadFile(src, des); err != nil {
		log.Errorf("write c1out file failed %v", err)
		return err
	}
	return nil
}

func checkMigrateFinishStateFile(fname string) error {
	existed, err := Exists(fname)
	if err != nil {
		return xerrors.New("check file failed:" + fname)
	}
	if !existed {
		return xerrors.New("file not exist:" + fname)
	}
	return nil
}

func checkMigrateFinishStateFileWithFTP(f *FtpClient, file string) error {
	return f.CheckFile(file)
}

func migrateErrCleanTargetFile(err *error, targetCacheDir, targetSealedFilePath, targetUnSealedFilePath string) {
	//if *err != nil {
	//	log.Errorf("migrate err [%s] remove target file[%s] ", (*err).Error(), targetCacheDir)
	//	if err := os.RemoveAll(targetCacheDir); err != nil {
	//		log.Error("RemoveAll failed：" + targetCacheDir)
	//	}
	//	if err := os.RemoveAll(targetSealedFilePath); err != nil {
	//		log.Error("RemoveAll failed：" + targetSealedFilePath)
	//	}
	//	if err := os.RemoveAll(targetUnSealedFilePath); err != nil {
	//		log.Error("RemoveAll failed：" + targetUnSealedFilePath)
	//	}
	//}
}

func MigrateFileWithFTP(task MigrateTasks, p abi.RegisteredSealProof, f *FtpClient) error {
	var deferErr error
	defer func() {
		//if r := recover(); r != nil {
		//	log.Error("migrateFile  recover", task.SectorID, r)
		//}
	}()
	workerPath := task.FromPath
	storePath := task.StorePath
	filePath := task.SectorID
	if filePath == "" {
		return xerrors.New("task.SectorID is null err:" + task.SectorID)
	}
	cacheFilePath := workerPath + "/cache/" + filePath
	existed, err := Exists(cacheFilePath)
	if err != nil {
		return xerrors.New("check file failed:" + cacheFilePath)
	}
	if !existed {
		return xerrors.New("file not exist:" + cacheFilePath)
	}

	sealedFilePath := workerPath + "/sealed/" + filePath
	existed, err = Exists(sealedFilePath)
	if err != nil {
		return xerrors.New("check file failed:" + sealedFilePath)
	}
	if !existed {
		return xerrors.New("file not exist:" + sealedFilePath)
	}
	targetFilePath := storePath
	targetCacheDir := targetFilePath + "/cache/" + filePath
	targetSealedFilePath := targetFilePath + "/sealed/"
	targetUnSealedFilePath := targetFilePath + "/unsealed/"

	err = f.Mkdir_P(targetUnSealedFilePath)
	if err != nil {
		deferErr = xerrors.New(targetUnSealedFilePath + " Mkdir_P err " + err.Error())
		return deferErr
	}

	toCopy := map[string]string{
		sealedFilePath:                        targetSealedFilePath + filePath,
		filepath.Join(cacheFilePath, "t_aux"): filepath.Join(targetCacheDir, "t_aux"),
		filepath.Join(cacheFilePath, "p_aux"): filepath.Join(targetCacheDir, "p_aux"),
	}
	info, ok := abi.SealProofInfos[p]
	if !ok {
		deferErr = xerrors.New("abi.SealProofInfos err")
		return deferErr
	}
	AddCachePathToCopy(toCopy, cacheFilePath, targetCacheDir, info.SectorSize)

	log.Infof("migrate worker file with ftp [%s] start", cacheFilePath)
	retries := 1
	for src, des := range toCopy {
		retries = 1
		for {
			err := f.UploadFileWithBreakpoint(src, des)
			if err != nil {
				log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", src, des, err)
				retries++
				if retries > 3 {
					deferErr = xerrors.New("copy 3 times error with " + err.Error())
					return deferErr
				} else {
					log.Infof("[%s]transfer worker file error:[%s], retry transfer proof [%d]times\n", src, err.Error(), retries)
					continue
				}
			}
			break
		}
	}

	toCheck := map[string]int64{
		targetSealedFilePath + filePath:        1,
		filepath.Join(targetCacheDir, "t_aux"): 0,
		filepath.Join(targetCacheDir, "p_aux"): 0,
	}
	if err := f.CheckAllFile(toCheck, targetCacheDir, p); err != nil {
		deferErr = xerrors.New("CheckAllFile " + targetCacheDir + ",failed: " + err.Error())
		return deferErr
	}

	if err := createMigrateFinishStateFileWithFTP(f, filepath.Join(cacheFilePath, MigrateFinishStat), filepath.Join(targetCacheDir, MigrateFinishStat)); err != nil {
		deferErr = xerrors.New("createMigrateFinishStateFileWithFTP " + filePath + ",failed: " + err.Error())
		return deferErr
	}

	log.Info("remove local transfer worker file: ", filePath)
	if err := os.RemoveAll(sealedFilePath); err != nil {
		log.Error("RemoveAll failed：" + sealedFilePath)
	}
	if err := os.RemoveAll(cacheFilePath); err != nil {
		log.Error("RemoveAll failed：" + cacheFilePath)
	}
	log.Infof("migrate worker file with ftp [%s] end", cacheFilePath)

	deferErr = nil
	return deferErr
}

func MigrateUpdateFile(task MigrateTasks, p abi.RegisteredSealProof) error {
	var deferErr error
	defer func() {
		if r := recover(); r != nil {
			log.Error("migrateFile  recover", task.SectorID, r)
		}
	}()
	cpBufSize := int64(0)
	if os.Getenv("MIGRATE_COPY_BUFFSIZE") != "" {
		s, err := strconv.ParseInt(os.Getenv("MIGRATE_COPY_BUFFSIZE"), 10, 64)
		if err != nil {
			s = 0
		}
		cpBufSize = int64(s)
	}
	workerPath := task.FromPath
	workerIP := task.FromIP
	storePath := task.StorePath
	storeIP := task.StoreIP
	if storePath == "" {
		storePath = workerPath
	}
	if storeIP != workerIP || storePath != workerPath {
		filePath := task.SectorID
		if filePath == "" {
			return xerrors.New("task.SectorID is null err:" + task.SectorID)
		}
		cacheFilePath := workerPath + "/update-cache/" + filePath
		existed, err := Exists(cacheFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + cacheFilePath)
		}
		if !existed {
			return xerrors.New("file not exist:" + cacheFilePath)
		}
		log.Infof("migrate worker file[%s] start", cacheFilePath)

		sealedFilePath := workerPath + "/update/" + filePath
		existed, err = Exists(sealedFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + sealedFilePath)
		}
		if !existed {
			return xerrors.New("file not exist:" + sealedFilePath)
		}

		words := strings.Split(storePath, "/")
		var lastIndex int
		for k, v := range words {
			if v != "" {
				lastIndex = k
			}
		}
		targetFilePath := filepath.Join(os.Getenv("MINER_SECTOR_REF_PATH"), storeIP, words[lastIndex])
		if storeIP == workerIP {
			targetFilePath = storePath
		}
		existed, err = Exists(targetFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + targetFilePath)
		}
		if !existed {
			return xerrors.New("file not exist：" + targetFilePath)
		}
		targetCacheDir := targetFilePath + "/update-cache/" + filePath
		targetSealedFilePath := targetFilePath + "/update/"

		if err := MakeDir(targetSealedFilePath); err != nil {
			return xerrors.New("MakeDir err :" + targetSealedFilePath + err.Error())
		}
		if err := MakeDir(targetCacheDir); err != nil {
			return xerrors.New("MakeDir err :" + targetCacheDir + err.Error())
		}
		retries := 1
		for {
			if _, err := CopyFile(sealedFilePath, targetSealedFilePath+filePath, int(cpBufSize)); err != nil {
				log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", sealedFilePath, targetSealedFilePath+filePath, err)
				retries++
				if retries > 3 {
					deferErr = xerrors.New("copy 3 times error with " + err.Error())
					return deferErr
				} else {
					log.Infof("transfer worker file error:[%s], retry transfer proof [%d]times\n", err.Error(), retries)
					continue
				}
			}
			break
		}
		toCopy := map[string]string{
			filepath.Join(cacheFilePath, "t_aux"): filepath.Join(targetCacheDir, "t_aux"),
			filepath.Join(cacheFilePath, "p_aux"): filepath.Join(targetCacheDir, "p_aux"),
		}
		info, ok := abi.SealProofInfos[p]
		if !ok {
			deferErr = xerrors.New("abi.SealProofInfos err")
			return deferErr
		}
		AddCachePathToCopy(toCopy, cacheFilePath, targetCacheDir, info.SectorSize)
		for src, des := range toCopy {
			retries = 1
			for {
				if _, err := CopyFile(src, des, 0); err != nil {
					log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", src, des, err)
					retries++
					if retries > 3 {
						deferErr = xerrors.New("copy 3 times error with " + err.Error())
						return deferErr
					} else {
						log.Infof("transfer worker file error:[%s], retry transfer proof [%d]times\n", err.Error(), retries)
						continue
					}
				}
				break
			}
		}

		toCheck := map[string]int64{
			targetSealedFilePath + filePath:        1,
			filepath.Join(targetCacheDir, "t_aux"): 0,
			filepath.Join(targetCacheDir, "p_aux"): 0,
		}
		if err := CheckAllFile(toCheck, targetCacheDir, p); err != nil {
			deferErr = xerrors.New("CheckAllFile " + targetCacheDir + ",failed: " + err.Error())
			return deferErr
		}

		if err := createMigrateFinishStateFile(filepath.Join(targetCacheDir, MigrateFinishStat)); err != nil {
			deferErr = xerrors.New("createMigrateFinishStateFile " + filePath + ",failed: " + err.Error())
			return deferErr
		}

		log.Info("remove local transfer worker file: ", filePath)
		if err := os.RemoveAll(sealedFilePath); err != nil {
			log.Error("RemoveAll failed：" + sealedFilePath)
		}
		if err := os.RemoveAll(cacheFilePath); err != nil {
			log.Error("RemoveAll failed：" + cacheFilePath)
		}
		log.Infof("migrate worker file[%s] end", cacheFilePath)
	}
	deferErr = nil
	return deferErr
}

func MigrateUpdateFileWithFTP(task MigrateTasks, p abi.RegisteredSealProof, f *FtpClient) error {
	var deferErr error
	defer func() {
		if r := recover(); r != nil {
			log.Error("migrateFile  recover", task.SectorID, r)
		}
	}()
	workerPath := task.FromPath
	storePath := task.StorePath
	filePath := task.SectorID
	if filePath == "" {
		return xerrors.New("task.SectorID is null err:" + task.SectorID)
	}
	cacheFilePath := workerPath + "/update-cache/" + filePath
	existed, err := Exists(cacheFilePath)
	if err != nil {
		return xerrors.New("check file failed:" + cacheFilePath)
	}
	if !existed {
		return xerrors.New("file not exist:" + cacheFilePath)
	}

	sealedFilePath := workerPath + "/update/" + filePath
	existed, err = Exists(sealedFilePath)
	if err != nil {
		return xerrors.New("check file failed:" + sealedFilePath)
	}
	if !existed {
		return xerrors.New("file not exist:" + sealedFilePath)
	}
	targetFilePath := storePath
	targetCacheDir := targetFilePath + "/update-cache/" + filePath
	targetSealedFilePath := targetFilePath + "/update/"

	toCopy := map[string]string{
		sealedFilePath:                        targetSealedFilePath + filePath,
		filepath.Join(cacheFilePath, "t_aux"): filepath.Join(targetCacheDir, "t_aux"),
		filepath.Join(cacheFilePath, "p_aux"): filepath.Join(targetCacheDir, "p_aux"),
	}
	info, ok := abi.SealProofInfos[p]
	if !ok {
		deferErr = xerrors.New("abi.SealProofInfos err")
		return deferErr
	}
	AddCachePathToCopy(toCopy, cacheFilePath, targetCacheDir, info.SectorSize)

	log.Infof("migrate worker file with ftp [%s] start", cacheFilePath)
	retries := 1
	for src, des := range toCopy {
		retries = 1
		for {
			err := f.UploadFileWithBreakpoint(src, des)
			if err != nil {
				log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", src, des, err)
				retries++
				if retries > 3 {
					deferErr = xerrors.New("copy 3 times error with " + err.Error())
					return deferErr
				} else {
					log.Infof("[%s]transfer worker file error:[%s], retry transfer proof [%d]times\n", src, err.Error(), retries)
					continue
				}
			}
			break
		}
	}

	toCheck := map[string]int64{
		targetSealedFilePath + filePath:        1,
		filepath.Join(targetCacheDir, "t_aux"): 0,
		filepath.Join(targetCacheDir, "p_aux"): 0,
	}
	if err := f.CheckAllFile(toCheck, targetCacheDir, p); err != nil {
		deferErr = xerrors.New("CheckAllFile " + targetCacheDir + ",failed: " + err.Error())
		return deferErr
	}

	if err := createMigrateFinishStateFileWithFTP(f, filepath.Join(cacheFilePath, MigrateFinishStat), filepath.Join(targetCacheDir, MigrateFinishStat)); err != nil {
		deferErr = xerrors.New("createMigrateFinishStateFileWithFTP " + filePath + ",failed: " + err.Error())
		return deferErr
	}

	log.Info("remove local transfer worker file: ", filePath)
	if err := os.RemoveAll(sealedFilePath); err != nil {
		log.Error("RemoveAll failed：" + sealedFilePath)
	}
	if err := os.RemoveAll(cacheFilePath); err != nil {
		log.Error("RemoveAll failed：" + cacheFilePath)
	}
	log.Infof("migrate worker file with ftp [%s] end", cacheFilePath)

	deferErr = nil
	return deferErr
}

func doMigrateTaskWithFtp(f *FtpClient, w *sync.WaitGroup, local *stores.Local, t MigrateTasks, spt abi.RegisteredSealProof) error {
	taskReq := MigrateRequest{
		SectorID:     t.SectorID,
		Version:      t.Version,
		FromIP:       t.FromIP,
		FromPath:     t.FromPath,
		StoreIP:      t.StoreIP,
		StorePath:    t.StorePath,
		FailureCount: t.FailureCount,
		FtpStatus:    t.FtpStatus,
		StartTime:    time.Now().Unix(),
		Error:        "",
		TaskType:     t.TaskType,
	}
	defer func() {
		taskReq.EndTime = time.Now().Unix()
		taskReq.CostTime = taskReq.EndTime - taskReq.StartTime
		//if err := reportMigrateResult(local, taskReq); err != nil {
		//	log.Error(t.SectorID, "reportMigrateResult err ", err)
		//}
		//w.Done()
	}()
	{
		IncreasingWait(int64(t.FailureCount))
		if taskReq.TaskType == MigrateType_Sealed {
			if err := MigrateFileWithFTP(t, spt, f); err != nil {
				taskReq.Success = false
				taskReq.Error = err.Error()
				taskReq.FailureCount = t.FailureCount + 1
				log.Error("migrateFile err ", err)
				return err
			}
		} else if taskReq.TaskType == MigrateType_Updated {
			if err := MigrateUpdateFileWithFTP(t, spt, f); err != nil {
				taskReq.Success = false
				taskReq.Error = err.Error()
				taskReq.FailureCount = t.FailureCount + 1
				log.Error("migrateFile err ", err)
				return err
			}
		} else {
			taskReq.Success = false
			taskReq.Error = t.SectorID + "task type is incorrect"
			taskReq.FailureCount = t.FailureCount + 1
			log.Error(t.SectorID, " task type is incorrect")
			return xerrors.New("task type is incorrect")
		}
	}
	taskReq.Success = true
	return nil
}

func IncreasingWait(failureCount int64) {
	waitTime := time.Duration(0)
	switch failureCount {
	case 0:
		waitTime = 0
	case 1:
		waitTime = 5 * time.Minute
	case 2:
		waitTime = 10 * time.Minute
	default:
		waitTime = 1 * time.Second
	}
	time.Sleep(waitTime)
}

func MigrateWithFtp(param MigrateParam, spt abi.RegisteredSealProof) error {
	//FtpIp 		:= os.Getenv("FtpIp")
	//FtpPort     := os.Getenv("FtpPort")
	//FtpUser     := os.Getenv("FtpUser")
	//FtpPassword := os.Getenv("FtpPassword")

	log.Info("MigrateWithFtp ")

	//m, err := StoreMachineManager.SelectStoreMachine(context.TODO(), NetWorkIOBalance, "")
	//if err != nil {
	//	log.Error("SelectStoreMachine err ", err)
	//	return err
	//}
	log.Info("machine.FtpEnv: ", param.FtpEnv)
	log.Info("machine.StorePath: ", param.StorePath)

	f := &FtpClient{
		ID:       "",
		Port:     param.FtpEnv.FtpPort,
		User:     param.FtpEnv.FtpUser,
		Password: param.FtpEnv.FtpPassword,
	}
	if err := f.ConnectFtpServer(param.StoreIP); err != nil {
		//SendFailResult(&wg, task, local, err)
		//continue
		log.Error("ConnectFtpServer err ", err)
		return err
	}

	//task := MigrateTasks{
	//	SectorID:     "s-t01000-0",
	//	Version:      "",
	//	FromIP:       "192.168.139.128",
	//	FromPath:     "/home/hp/.genesis-sectors",
	//	StoreIP:      m.StoreIP,
	//	StorePath:    m.StorePath,
	//	FailureCount: 0,
	//	FtpStatus:    0,
	//	StartTime:    time.Now().Unix(),
	//	Error:        "",
	//	TaskType:     MigrateType_Sealed,
	//}
	task := MigrateTasks{
		SectorID:     param.SectorID,
		Version:      "",
		FromIP:       param.FromIP,
		FromPath:     param.FromPath,
		StoreIP:      param.StoreIP,
		StorePath:    param.StorePath,
		FailureCount: 0,
		FtpStatus:    0,
		StartTime:    time.Now().Unix(),
		Error:        "",
		TaskType:     MigrateType_Sealed,
	}
	insertResult, err := AddMigrateTask(context.TODO(), &task)
	if err != nil {
		log.Error("AddMigrateTask error")
		return err
	}
	//wg := sync.WaitGroup{}
	//wg.Add(1)
	err = doMigrateTaskWithFtp(f, nil, nil, task, spt)
	//wg.Wait()
	if err != nil {
		task.Error = err.Error()
	} else {
		task.Error = "success"
	}
	objID := insertResult.InsertedID.(primitive.ObjectID)
	err = UpdateMigrateTaskByID(context.TODO(), objID, &task)

	return nil
}

func MigrateFile(task MigrateTasks, p abi.RegisteredSealProof) error {
	var deferErr error
	defer func() {
		//if r := recover(); r != nil {
		//	log.Error("migrateFile  recover", task.SectorID, r)
		//}
	}()
	cpBufSize := int64(0)
	if os.Getenv("MIGRATE_COPY_BUFFSIZE") != "" {
		s, err := strconv.ParseInt(os.Getenv("MIGRATE_COPY_BUFFSIZE"), 10, 64)
		if err != nil {
			s = 0
		}
		cpBufSize = int64(s)
	}
	workerPath := task.FromPath
	workerIP := task.FromIP
	storePath := task.StorePath
	storeIP := task.StoreIP
	if storePath == "" {
		storePath = workerPath
	}
	if storeIP != workerIP || storePath != workerPath {
		filePath := task.SectorID
		if filePath == "" {
			return xerrors.New("task.SectorID is null err:" + task.SectorID)
		}
		cacheFilePath := workerPath + "/cache/" + filePath
		existed, err := Exists(cacheFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + cacheFilePath)
		}
		if !existed {
			return xerrors.New("file not exist:" + cacheFilePath)
		}
		log.Infof("migrate worker file[%s] start", cacheFilePath)

		sealedFilePath := workerPath + "/sealed/" + filePath
		existed, err = Exists(sealedFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + sealedFilePath)
		}
		if !existed {
			return xerrors.New("file not exist:" + sealedFilePath)
		}

		//words := strings.Split(storePath, "/")
		//var lastIndex int
		//for k, v := range words {
		//	if v != "" {
		//		lastIndex = k
		//	}
		//}
		//targetFilePath := filepath.Join(os.Getenv("MINER_SECTOR_REF_PATH"), storeIP, words[lastIndex])
		targetFilePath := filepath.Join(storePath, storeIP)
		if storeIP == workerIP {
			targetFilePath = storePath
		}
		existed, err = Exists(targetFilePath)
		if err != nil {
			return xerrors.New("check file failed:" + targetFilePath)
		}
		if !existed {
			return xerrors.New("file not exist：" + targetFilePath)
		}
		targetCacheDir := targetFilePath + "/cache/" + filePath
		targetSealedFilePath := targetFilePath + "/sealed/"
		targetUnSealedFilePath := targetFilePath + "/unsealed/"

		if err := MakeDir(targetUnSealedFilePath); err != nil {
			return xerrors.New("MakeDir err :" + targetUnSealedFilePath + err.Error())
		}
		if err := MakeDir(targetSealedFilePath); err != nil {
			return xerrors.New("MakeDir err :" + targetSealedFilePath + err.Error())
		}
		if err := MakeDir(targetCacheDir); err != nil {
			return xerrors.New("MakeDir err :" + targetCacheDir + err.Error())
		}
		//defer migrateErrCleanTargetFile(&deferErr, targetCacheDir, targetSealedFilePath + filePath, targetUnSealedFilePath + filePath)
		retries := 1
		for {
			if _, err := CopyFile(sealedFilePath, targetSealedFilePath+filePath, int(cpBufSize)); err != nil {
				log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", sealedFilePath, targetSealedFilePath+filePath, err)
				retries++
				if retries > 3 {
					deferErr = xerrors.New("copy 3 times error with " + err.Error())
					return deferErr
				} else {
					log.Infof("transfer worker file error:[%s], retry transfer proof [%d]times\n", err.Error(), retries)
					continue
				}
			}
			break
		}
		toCopy := map[string]string{
			filepath.Join(cacheFilePath, "t_aux"): filepath.Join(targetCacheDir, "t_aux"),
			filepath.Join(cacheFilePath, "p_aux"): filepath.Join(targetCacheDir, "p_aux"),
		}
		info, ok := abi.SealProofInfos[p]
		if !ok {
			deferErr = xerrors.New("abi.SealProofInfos err")
			return deferErr
		}
		AddCachePathToCopy(toCopy, cacheFilePath, targetCacheDir, info.SectorSize)
		for src, des := range toCopy {
			retries = 1
			for {
				if _, err := CopyFile(src, des, 0); err != nil {
					log.Errorf("CopyFile [%s] to [%s] failed:[%v]\n", src, des, err)
					retries++
					if retries > 3 {
						deferErr = xerrors.New("copy 3 times error with " + err.Error())
						return deferErr
					} else {
						log.Infof("transfer worker file error:[%s], retry transfer proof [%d]times\n", err.Error(), retries)
						continue
					}
				}
				break
			}
		}

		toCheck := map[string]int64{
			targetSealedFilePath + filePath:        1,
			filepath.Join(targetCacheDir, "t_aux"): 0,
			filepath.Join(targetCacheDir, "p_aux"): 0,
		}
		if err := CheckAllFile(toCheck, targetCacheDir, p); err != nil {
			deferErr = xerrors.New("CheckAllFile " + targetCacheDir + ",failed: " + err.Error())
			return deferErr
		}

		if err := createMigrateFinishStateFile(filepath.Join(targetCacheDir, MigrateFinishStat)); err != nil {
			deferErr = xerrors.New("createMigrateFinishStateFile " + filePath + ",failed: " + err.Error())
			return deferErr
		}

		log.Info("remove local transfer worker file: ", filePath)
		if err := os.RemoveAll(sealedFilePath); err != nil {
			log.Error("RemoveAll failed：" + sealedFilePath)
		}
		if err := os.RemoveAll(cacheFilePath); err != nil {
			log.Error("RemoveAll failed：" + cacheFilePath)
		}
		log.Infof("migrate worker file[%s] end", cacheFilePath)
	}
	deferErr = nil
	return deferErr
}

func doMigrateTask(w *sync.WaitGroup, local *stores.Local, t MigrateTasks, spt abi.RegisteredSealProof) {
	taskReq := MigrateRequest{
		SectorID:     t.SectorID,
		Version:      t.Version,
		FromIP:       t.FromIP,
		FromPath:     t.FromPath,
		StoreIP:      t.StoreIP,
		StorePath:    t.StorePath,
		FailureCount: t.FailureCount,
		StartTime:    time.Now().Unix(),
		Error:        "",
		TaskType:     t.TaskType,
	}
	defer func() {
		taskReq.EndTime = time.Now().Unix()
		taskReq.CostTime = taskReq.EndTime - taskReq.StartTime
		//if err := reportMigrateResult(local, taskReq); err != nil {
		//	log.Error(t.SectorID, "reportMigrateResult err ", err)
		//}
		w.Done()
	}()
	{
		IncreasingWait(int64(t.FailureCount))
		if taskReq.TaskType == MigrateType_Sealed {
			if err := MigrateFile(t, spt); err != nil {
				taskReq.Success = false
				taskReq.Error = err.Error()
				taskReq.FailureCount = t.FailureCount + 1
				log.Error("migrateFile err ", err)
				return
			}
		} else if taskReq.TaskType == MigrateType_Updated {
			if err := MigrateUpdateFile(t, spt); err != nil {
				taskReq.Success = false
				taskReq.Error = err.Error()
				taskReq.FailureCount = t.FailureCount + 1
				log.Error("migrateFile err ", err)
				return
			}
		} else {
			taskReq.Success = false
			taskReq.Error = t.SectorID + "task type is incorrect"
			taskReq.FailureCount = t.FailureCount + 1
			log.Error(t.SectorID, " task type is incorrect")
			return
		}
	}
	taskReq.Success = true
	if os.Getenv("LOCAL_STORE") == "1" {
		taskReq.StoreIP = "127.0.0.1"
	}
	return
}

func MigrateWithNfs(local *stores.Local, spt abi.RegisteredSealProof) error {
	wg := sync.WaitGroup{}
	wg.Add(1)
	task := MigrateTasks{
		SectorID:     "s-t01000-0",
		Version:      "",
		FromIP:       "192.168.139.128",
		FromPath:     "/home/hp/.genesis-sectors",
		StoreIP:      "192.168.0.11",
		StorePath:    "/home/hp/nfs",
		FailureCount: 0,
		FtpStatus:    0,
		StartTime:    time.Now().Unix(),
		Error:        "",
		TaskType:     MigrateType_Sealed,
	}
	go doMigrateTask(&wg, local, task, spt)
	wg.Wait()

	return nil
}
