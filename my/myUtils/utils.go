package myUtils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
)

func GetFunctionName(i interface{}, seps ...rune) string {
	// 获取函数名称
	fn := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()

	// 用 seps 进行分割
	fields := strings.FieldsFunc(fn, func(sep rune) bool {
		for _, s := range seps {
			if sep == s {
				return true
			}
		}
		return false
	})

	// fmt.Println(fields)

	if size := len(fields); size > 0 {
		return fields[size-1]
	}
	return ""
}

func WriteFileString(out string) {
	file, err := os.OpenFile("/home/lotus/result.txt", os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("文件打开失败", err)
	}
	defer file.Close()
	//写入文件时，使用带缓存的 *Writer
	write := bufio.NewWriter(file)
	write.WriteString(out + "\n")
	//Flush将缓存的文件真正写入到文件中
	write.Flush()
}

func Interface2Json(i interface{}) (string, error) {
	marshal, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(marshal), nil
}

func GetLocalIPv4s() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}

	for _, a := range addrs {
		// 检查ip地址判断是否回环地址
		if ipNet, ok := a.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			if strings.HasPrefix(ipNet.IP.String(), "192.168") {
				return ipNet.IP.String()
			}
		}
	}

	return ""
}
