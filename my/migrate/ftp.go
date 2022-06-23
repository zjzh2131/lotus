package migration

import (
	"bytes"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	//"github.com/filecoin-project/lotus/extern/sector-storage/utils/file"
	"github.com/jlaffaye/ftp"
	"golang.org/x/xerrors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type FtpClient struct {
	ID       string
	_ftp     *ftp.ServerConn
	Port     string
	User     string
	Password string
}

type FtpEnvStruct struct {
	FtpPort     string `json:"FtpPort" bson:"FtpPort"`
	FtpUser     string `json:"FtpUser" bson:"FtpUser"`
	FtpPassword string `json:"FtpPassword" bson:"FtpPassword"`
}

func CheckFtpEnv(ftpEnv FtpEnvStruct) error {
	if ftpEnv.FtpPort == "" || ftpEnv.FtpUser == "" || ftpEnv.FtpPassword == "" {
		return xerrors.New("ftp env not set")
	}
	return nil
}

func CheckFtpServerOnline(storeip string, ftpEnv FtpEnvStruct) error {
	f, err := ftp.Dial(storeip+":"+ftpEnv.FtpPort, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return err
	}
	err = f.Login(ftpEnv.FtpUser, ftpEnv.FtpPassword)
	if err != nil {
		return err
	}
	if err := f.Quit(); err != nil {
		return err
	}
	return nil
}

func (f *FtpClient) CheckFtpServerRegularWork(storeip, stroePath string) (err error) {
	ftpPort := f.Port
	ftpUser := f.User
	ftpPassword := f.Password
	f._ftp, err = ftp.Dial(storeip+":"+ftpPort, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return err
	}
	err = f._ftp.Login(ftpUser, ftpPassword)
	if err != nil {
		return err
	}
	srcPath := filepath.Join("./TestFtpWorkFileTag")
	desPath := filepath.Join(stroePath, "TestFtpWorkFileTag")
	if err := WriteDataToFile(srcPath, []byte("")); err != nil {
		return err
	}
	if err := f.UploadFile(srcPath, desPath); err != nil {
		return err
	}
	if err := f._ftp.Quit(); err != nil {
		return err
	}
	return nil
}

func (f *FtpClient) FtpHandler() *ftp.ServerConn {
	return f._ftp
}

func (f *FtpClient) ConnectFtpServer(storeip string) (err error) {
	ftpPort := f.Port
	ftpUser := f.User
	ftpPassword := f.Password
	f._ftp, err = ftp.Dial(storeip+":"+ftpPort, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return err
	}
	err = f._ftp.Login(ftpUser, ftpPassword)
	if err != nil {
		return err
	}
	return nil
}

func (f *FtpClient) DisconnectFtpServer() error {
	if err := f._ftp.Quit(); err != nil {
		return err
	}
	return nil
}

func (f *FtpClient) Mkdir_P(path string) error {
	if f._ftp == nil {
		return xerrors.New("ftp client not init")
	}
	words := strings.Split(path, "/")
	if len(words) == 0 {
		return xerrors.New("path format wrong")
	}
	var tmpDir string
	for k, _ := range words {
		if k == 0 || len(words)-1 == k {
			continue
		}
		tmpDir = tmpDir + "/" + words[k]
		_ = f._ftp.MakeDir(tmpDir)
	}
	return nil
}

func (f *FtpClient) UploadFileWithBreakpoint(src, des string) error {
	err := f.Mkdir_P(des)
	if err != nil {
		return err
	}
	trgSize, err := f._ftp.FileSize(des)
	if err != nil {
		trgSize = 0
	}
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	fi, _ := srcFile.Stat()
	srcSize := fi.Size()
	defaultSize := int64(10)
	if os.Getenv("FTP_BUF_SIZE") != "" {
		bs, err := strconv.ParseInt(os.Getenv("FTP_BUF_SIZE"), 10, 64)
		if err != nil {
			bs = 10
		}
		defaultSize = int64(bs)
	}
	bufSize := int64(defaultSize * 1024 * 1024) //10M
	if trgSize > srcSize {
		return xerrors.New(fmt.Sprintf("src file[%s] size [%d] less than target file[%s] size[%d]", src, srcSize, des, trgSize))
	}
	for {
		if trgSize == srcSize {
			break
		}
		if srcSize-trgSize < bufSize {
			bufSize = srcSize - trgSize
		}
		//log.Errorf("buf size [%d] trgsize [%d]", bufSize, trgSize)
		if bufSize == srcSize {
			err = f._ftp.Stor(des, srcFile)
			if err != nil {
				return err
			}
		} else {
			bufData := make([]byte, bufSize)
			data := bytes.NewBuffer(bufData)
			if _, err := srcFile.Seek(trgSize, 0); err != nil {
				return xerrors.New(fmt.Sprintf("src file[%s] Seek offset error[%s]", src, err.Error()))
			}
			_, err := srcFile.Read(data.Bytes())
			if err != nil {
				return xerrors.New(fmt.Sprintf("src file[%s] Read data error[%s]", src, err.Error()))
			}
			err = f._ftp.StorFrom(des, data, uint64(trgSize))
			if err != nil {
				return xerrors.New(fmt.Sprintf("src file[%s] StorFrom error[%s]", src, err.Error()))
			}
		}
		trgSize, err = f._ftp.FileSize(des)
		if err != nil {
			return xerrors.New(fmt.Sprintf("des file[%s] get FileSize error[%s]", des, err.Error()))
		}
	}

	storeSize, err := f._ftp.FileSize(des)
	if err != nil {
		return err
	}
	if srcSize != storeSize {
		return xerrors.New(fmt.Sprintf("src file[%s] size [%d] not equal target file[%s] size[%d]", src, srcSize, des, storeSize))
	}
	return nil
}

func (f *FtpClient) UploadFile(src, des string) error {
	err := f.Mkdir_P(des)
	if err != nil {
		return err
	}
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	fi, _ := srcFile.Stat()
	srcSize := fi.Size()
	defer srcFile.Close()
	err = f._ftp.Stor(des, srcFile)
	if err != nil {
		return err
	}
	storeSize, err := f._ftp.FileSize(des)
	if err != nil {
		return err
	}
	if srcSize != storeSize {
		return xerrors.New(fmt.Sprintf("src file[%s] size [%d] not equal target file[%s] size[%d]", src, srcSize, des, storeSize))
	}
	return nil
}

func (f *FtpClient) CheckFile(file string) error {
	_, err := f._ftp.FileSize(file)
	if err != nil {
		return err
	}
	return nil
}

func (f *FtpClient) CheckAllFile(toCheck map[string]int64, cacheDir string, p abi.RegisteredSealProof) error {
	info, ok := abi.SealProofInfos[p]
	if !ok {
		return xerrors.New("abi.SealProofInfos err")
	}
	AddCachePathsForSectorSize(toCheck, cacheDir, info.SectorSize)

	for p, sz := range toCheck {
		st, err := f._ftp.FileSize(p)
		if err != nil {
			return err
		}
		if sz == 1 {
			if st != int64(info.SectorSize)*sz {
				return xerrors.Errorf("[%s] wrong file size ", p)
			}
		}
		if sz == 0 {
			if st == 0 {
				return xerrors.Errorf("[%s] invalid file size 0 ", p)
			}
		}
	}
	return nil
}
