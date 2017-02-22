package message

import (
	"bytes"
	"fmt"
	"strings"
)

var (
	Type_Redis_Get     = "GET"
	Type_Redis_Set     = "SET"
	Type_Redis_Exists  = "EXISTS"
	Type_Redis_Del     = "DEL"
	Type_Redis_Hget    = "HGET"
	Type_Redis_Hset    = "HSET"
	Type_Redis_Hmget   = "HMGET"
	Type_Redis_Hmset   = "HMSET"
	Type_Redis_Hexists = "HEXISTS"

	KeySplitDelim   = byte('|')
	HbaseSplitDelim = byte(':')
)

type HedisWrapMessage struct {
	Command string   // 协议类型,即是redis的命令类型
	Key     []byte   // redis的key值
	ExtData [][]byte // 数据部分

	// 以下是从key中解析出来的hbase相关参数
	HbaseNameSpace []byte // key中解析出来的制定Hbase的namespace
	Table          []byte // key中解析出来的制定Hbase的tablename
	ColumnFamily   []byte // key中解析出来的制定Hbase的column family字段
}

func NewHedisWrapMessage() *HedisWrapMessage {
	return &HedisWrapMessage{}
}

func (this *HedisWrapMessage) Wrap(message *Message) (err error) {
	if len(message.BytesArrays) == 0 || len(message.BytesArrays) != int(message.Len) {
		return fmt.Errorf("parse message arrays error")
	}
	var (
		bytesValue []byte
		command    string
		i          int
	)
	this.ExtData = make([][]byte, 0)
	for i, bytesValue = range message.BytesArrays {
		if i == 0 { // 解析command部分
			command = strings.ToUpper(string(bytesValue))
			if command == Type_Redis_Get || command == Type_Redis_Set ||
				command == Type_Redis_Del || command == Type_Redis_Exists ||
				command == Type_Redis_Hget || command == Type_Redis_Hset ||
				command == Type_Redis_Hmget || command == Type_Redis_Hmset ||
				command == Type_Redis_Hexists {
				this.Command = command
			} else {
				return fmt.Errorf("invalid redis command %v", command)
			}
			continue
		}
		if i == 1 { // 解析key部分
			if err = this.parseKey(bytesValue); err != nil {
				return
			}
			continue
		}
		this.ExtData = append(this.ExtData, bytesValue)
	}
	return
}

func (this *HedisWrapMessage) parseKey(bytesValue []byte) (err error) {
	if len(bytesValue) == 0 {
		err = fmt.Errorf("key is null")
		return
	}
	if index := bytes.IndexByte(bytesValue, KeySplitDelim); index >= 0 {
		this.Key = bytesValue[index+1:]
		hbaseParts := bytes.Split(bytesValue[:index], []byte{HbaseSplitDelim})
		hbasePartsLen := len(hbaseParts)
		if hbasePartsLen == 1 {
			if len(hbaseParts[0]) > 0 { // 没有设置namespace, table, column family
				this.Table = hbaseParts[0]
			}
		} else if hbasePartsLen == 2 { // 只设置的namespace, table
			this.HbaseNameSpace, this.Table = hbaseParts[0], hbaseParts[1]
		} else if hbasePartsLen == 3 {
			this.HbaseNameSpace, this.Table, this.ColumnFamily = hbaseParts[0], hbaseParts[1], hbaseParts[2]
		}
	} else {
		this.Key = bytesValue
	}
	return
}

// redis 读取 协议封装类
type HRedisBox struct {
	Command   string
	Key       []byte
	Others    [][]byte
	TableName []byte
}
