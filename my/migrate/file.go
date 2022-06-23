package migration

import (
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

func HomeDir() string {
	p, _ := homedir.Expand("~/")
	return p
}

func WriteDataToFile(filePath string, data []byte) error {
	exists, err := Exists(path.Dir(filePath))
	if err != nil {
		return xerrors.Errorf("check filePath %s failed", filePath)
	}

	if !exists {
		if err := os.MkdirAll(path.Dir(filePath), 0755); err != nil {
			return xerrors.Errorf("mkdirAll failed, %w", err)
		}
	}

	err = ioutil.WriteFile(filePath, data, 0666)
	if err != nil {
		return xerrors.Errorf("write file failed, %w", err)
	}

	return nil
}

func FetchDataFromFile(filePath string) ([]byte, error) {
	exists, err := Exists(filePath)
	if err != nil {
		return nil, xerrors.Errorf("check filePath %s failed", filePath)
	}
	if !exists {
		return nil, xerrors.Errorf("file %s not exist", filePath)
	}

	contents, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, xerrors.New("load file failed")
	}

	return contents, err
}

func Exists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func CopyDir(srcPath, desPath string) error {
	if srcInfo, err := os.Stat(srcPath); err != nil {
		return err
	} else {
		if !srcInfo.IsDir() {
			return xerrors.New("source path is not the correct directory")
		}
	}

	if desInfo, err := os.Stat(desPath); err != nil {
		return err
	} else {
		if !desInfo.IsDir() {
			return xerrors.New(" destination path is not the correct directory ")
		}
	}

	if strings.TrimSpace(srcPath) == strings.TrimSpace(desPath) {
		return xerrors.New("source path cannot be the same as the target path ")
	}

	err := filepath.Walk(srcPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if path == srcPath {
			return nil
		}

		destNewPath := strings.Replace(path, srcPath, desPath, -1)

		if !f.IsDir() {
			if _, err := CopyFile(path, destNewPath, 0); err != nil {
				return err
			}
		} else {
			if !FileIsExisted(destNewPath) {
				return MakeDir(destNewPath)
			}
		}

		return nil
	})

	return err
}

func FileIsExisted(filename string) bool {
	existed := true
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		existed = false
	}
	return existed
}

func MakeDir(dir string) error {
	if !FileIsExisted(dir) {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return err
		}
	}
	return nil
}

func CopyFile(src, des string, bufSize int) (written int64, err error) {
	if os.Getenv("LOCAL_STORE") == "1" {
		cpCmd := "cp " + src + " " + des
		c := exec.Command("/bin/sh", "-c", cpCmd)
		c.Stdin = os.Stdin
		c.Stderr = os.Stderr
		c.Stdout = os.Stdout
		err = c.Run()
		if err != nil {
			return 0, err
		}
		return 0, nil
	}

	if bufSize <= 0 {
		bufSize = 10 * 1024 * 1024 //10M
	}
	buf := make([]byte, bufSize)

	srcFile, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	fi, _ := srcFile.Stat()
	perm := fi.Mode()

	desFile, err := os.OpenFile(des, os.O_CREATE|os.O_RDWR|os.O_TRUNC, perm)
	if err != nil {
		return 0, err
	}
	defer desFile.Close()

	count := 0
	for {
		n, err := srcFile.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		if n == 0 {
			break
		}

		if wn, err := desFile.Write(buf[:n]); err != nil {
			return 0, err
		} else {
			count += wn
		}
	}

	return int64(count), nil
}

func CheckAllFile(toCheck map[string]int64, cacheDir string, p abi.RegisteredSealProof) error {
	info, ok := abi.SealProofInfos[p]
	if !ok {
		return xerrors.New("abi.SealProofInfos err")
	}
	AddCachePathsForSectorSize(toCheck, cacheDir, info.SectorSize)

	for p, sz := range toCheck {
		st, err := os.Stat(p)
		if err != nil {
			return err
		}
		if sz == 1 {
			if st.Size() != int64(info.SectorSize)*sz {
				return xerrors.Errorf("[%s] wrong file size ", p)
			}
		}
		if sz == 0 {
			if st.Size() == 0 {
				return xerrors.Errorf("[%s] invalid file size 0 ", p)
			}
		}
	}
	return nil
}

func AddCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		fmt.Printf("not checking cache files of %s sectors for faults \n", ssize)
	}
}

func AddCachePathToCopy(toCopy map[string]string, srcDir, desDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		toCopy[filepath.Join(srcDir, "sc-02-data-tree-r-last.dat")] = filepath.Join(desDir, "sc-02-data-tree-r-last.dat")
	case 32 << 30:
		for i := 0; i < 8; i++ {
			toCopy[filepath.Join(srcDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = filepath.Join(desDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			toCopy[filepath.Join(srcDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = filepath.Join(desDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))
		}
	default:
		fmt.Printf("not checking cache files of %s sectors for faults \n", ssize)
	}
}
