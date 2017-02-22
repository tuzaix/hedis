package hproto

import (
	"fmt"
	"hbase"
	"message"
)

var _ CommandProto = (*RedisHSet)(nil)
var _ CommandProto = (*RedisHGet)(nil)
var _ CommandProto = (*RedisHExists)(nil)
var _ CommandProto = (*RedisHmGet)(nil)
var _ CommandProto = (*RedisHmSet)(nil)

type RedisHSet struct {
	BaseRedisAction
}

func NewRedisHSet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisHSet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisHSet) Check() (err error) {
	return this.checkNotEqual(2)
}
func (this *RedisHSet) BSave() (lines []string, err error) {
	columnMaps := map[string][]byte{string(this.WrapMessage.ExtData[0]): this.WrapMessage.ExtData[1]}
	if err = hbase.HMSet(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, columnMaps); err == nil {
		lines = ActionOK
	}
	return
}

//----------------------------------------------------
type RedisHGet struct {
	BaseRedisAction
}

func NewRedisHGet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisHGet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisHGet) Check() (err error) {
	return this.checkNotEqual(1)
}
func (this *RedisHGet) BSave() (lines []string, err error) {
	var columnValues map[string][]byte
	if columnValues, err = hbase.HMGet(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, this.WrapMessage.ExtData); err == nil {
		if v, ok := columnValues[string(this.WrapMessage.ExtData[0])]; ok {
			lines = []string{fmt.Sprintf("$%d\r\n%s\r\n", len(v), string(v))}
		} else {
			lines = HGetErr
		}
	} else if err == hbase.ErrNullValue {
		lines = HGetErr
		err = nil
	}
	return
}

//----------------------------------------------------
type RedisHExists struct {
	BaseRedisAction
}

func NewRedisHExists(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisHExists{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisHExists) Check() (err error) {
	return this.checkNotEqual(1)
}
func (this *RedisHExists) BSave() (lines []string, err error) {
	var ok bool
	if ok, err = hbase.HExists(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, this.WrapMessage.ExtData[0]); err == nil && ok {
		lines = ExistsOK
	} else if !ok && err == nil {
		lines = ExistsErr
	}
	return
}

//----------------------------------------------------
type RedisHmSet struct {
	BaseRedisAction
}

func NewRedisHmSet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisHmSet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisHmSet) Check() (err error) {
	if err = this.checkGreaterThan(0); err != nil {
		return
	}
	if len(this.WrapMessage.ExtData)%2 != 0 {
		err = fmt.Errorf("wrong number of arguments for '%s' command", this.WrapMessage.Command)
	}
	return
}
func (this *RedisHmSet) BSave() (lines []string, err error) {
	var (
		columnMaps map[string][]byte = make(map[string][]byte)
		key        string
		i          int
		v          []byte
	)
	for i, v = range this.WrapMessage.ExtData {
		if i%2 == 0 {
			key = string(v)
		} else {
			columnMaps[key] = v
		}
	}
	if err = hbase.HMSet(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, columnMaps); err == nil {
		lines = ActionOK
	}
	return
}

//----------------------------------------------------
type RedisHmGet struct {
	BaseRedisAction
}

func NewRedisHmGet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisHmGet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisHmGet) Check() (err error) {
	err = this.checkGreaterThan(0)
	return
}
func (this *RedisHmGet) BSave() (lines []string, err error) {
	var (
		columnValues map[string][]byte
		columnNum    int = len(this.WrapMessage.ExtData)
		column       []byte
		value        []byte
		i            int
		ok           bool
	)
	lines = make([]string, columnNum+1)
	lines[0] = fmt.Sprintf("*%d\r\n", columnNum)
	if columnValues, err = hbase.HMGet(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, this.WrapMessage.ExtData); err == nil {
		for i, column = range this.WrapMessage.ExtData {
			if value, ok = columnValues[string(column)]; ok {
				lines[i+1] = fmt.Sprintf("$%d\r\n%s\r\n", len(value), string(value))
			} else {
				lines[i+1] = "$-1\r\n"
			}
		}
	} else if err == hbase.ErrNullValue {
		for i, _ = range this.WrapMessage.ExtData {
			lines[i+1] = "$-1\r\n"
		}
		err = nil
	}
	return
}
