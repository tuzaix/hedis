package hproto

import (
	"fmt"
	"hbase"
	"message"
)

var _ CommandProto = (*RedisSet)(nil)
var _ CommandProto = (*RedisGet)(nil)
var _ CommandProto = (*RedisDel)(nil)
var _ CommandProto = (*RedisExists)(nil)

type RedisSet struct {
	BaseRedisAction
}

func NewRedisSet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisSet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisSet) Check() (err error) {
	return this.checkNotEqual(1)
}
func (this *RedisSet) BSave() (lines []string, err error) {
	if err = hbase.Set(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key, this.WrapMessage.ExtData[0]); err == nil {
		lines = ActionOK
	}
	return
}

// -------------------------------------------------------
type RedisGet struct {
	BaseRedisAction
}

func NewRedisGet(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisGet{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisGet) Check() (err error) {
	return this.checkNotEqual(0)
}
func (this *RedisGet) BSave() (lines []string, err error) {
	var bytesValue []byte
	if bytesValue, err = hbase.Get(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key); err == nil {
		lines = []string{fmt.Sprintf("$%d\r\n%s\r\n", len(bytesValue), string(bytesValue))}
	} else if err == hbase.ErrNullValue {
		err = nil
		lines = HGetErr
	}
	return
}

// -------------------------------------------------------
type RedisDel struct {
	BaseRedisAction
}

func NewRedisDel(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisDel{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisDel) Check() (err error) {
	return this.checkNotEqual(0)
}
func (this *RedisDel) BSave() (lines []string, err error) {
	if err = hbase.Del(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key); err == nil {
		lines = DelOK
	} else if err != hbase.ErrGetConnTimeout {
		lines = DelErr
		err = nil
	}
	return
}

//-----------------------------------------------------------
type RedisExists struct {
	BaseRedisAction
}

func NewRedisExists(wrap *message.HedisWrapMessage) CommandProto {
	return &RedisExists{BaseRedisAction{WrapMessage: wrap}}
}
func (this *RedisExists) Check() (err error) {
	return this.checkNotEqual(0)
}
func (this *RedisExists) BSave() (lines []string, err error) {
	var ok bool
	if ok, err = hbase.Exists(this.WrapMessage.HbaseNameSpace, this.WrapMessage.Table, this.WrapMessage.ColumnFamily, this.WrapMessage.Key); err == nil && ok {
		lines = ExistsOK
	} else if !ok && err == nil {
		lines = ExistsErr
	}
	return
}
