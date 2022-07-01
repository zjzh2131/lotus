package myUtils

import (
	"github.com/bwmarrin/snowflake"
	"strconv"
	"strings"
)

var Node *snowflake.Node

func init() {
	var err error
	ipLast := strings.Split(GetLocalIPv4s(), ".")[3]
	ipNodeId, err := strconv.Atoi(ipLast)
	if err != nil {
		panic("ipLast to ipNodeId failed")
	}
	Node, err = snowflake.NewNode(int64(ipNodeId))
	if err != nil {
		panic("init snowflake failed")
	}
}

func GenSnowID() int64 {
	return Node.Generate().Int64()
}

func GenSnowIDStr() string {
	return Node.Generate().String()
}
