package hproto

import (
	"common"
	"errors"
	"fmt"
	"message"
	"net"
	"runtime"
)

var (
	ActionOK = []string{"+OK\r\n"}

	ExistsOK  = []string{":1\r\n"}
	ExistsErr = []string{":0\r\n"}

	DelOK  = []string{":1\r\n"}
	DelErr = []string{":0\r\n"}

	HGetErr = []string{"$-1\r\n"}

	ErrPanic = errors.New("unknown panic error")
)

type CommandProto interface {
	Check() error
	BSave() ([]string, error)
	Print()
}

type BaseRedisAction struct {
	WrapMessage *message.HedisWrapMessage
}

func (this *BaseRedisAction) Print() {
	fmt.Println("------------------------")
	fmt.Println("command:", this.WrapMessage.Command)
	fmt.Println("key:", string(this.WrapMessage.Key))
	fmt.Println("data length:", len(this.WrapMessage.ExtData))
	for i, v := range this.WrapMessage.ExtData {
		fmt.Println(i, string(v))
	}
	fmt.Println("------------------------")
}

func (this *BaseRedisAction) checkBase() (err error) {
	if len(this.WrapMessage.Key) == 0 {
		err = fmt.Errorf(" unknown or null command '%s'", this.WrapMessage.Command)
	}
	return
}

func (this *BaseRedisAction) checkNotEqual(paramNum int) (err error) {
	if err = this.checkBase(); err != nil {
		return
	}
	if len(this.WrapMessage.ExtData) != paramNum {
		err = fmt.Errorf(" wrong number of arguments for '%s' command", this.WrapMessage.Command)
	}
	return
}

func (this *BaseRedisAction) checkGreaterThan(paramNum int) (err error) {
	if err = this.checkBase(); err != nil {
		return
	}
	if len(this.WrapMessage.ExtData) < paramNum {
		err = fmt.Errorf(" wrong number of arguments for '%s' command", this.WrapMessage.Command)
	}
	return
}

func (this *BaseRedisAction) checkLessThan(paramNum int) (err error) {
	if err = this.checkBase(); err != nil {
		return
	}
	if len(this.WrapMessage.ExtData) > paramNum {
		err = fmt.Errorf(" wrong number of arguments for '%s' command", this.WrapMessage.Command)
	}
	return
}

func Exec(wrap *message.HedisWrapMessage, conn net.Conn) (lines []string, err error) {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 4096)
			runtime.Stack(buf, true)
			common.Errorf("client %v handle fatal %v %v", conn.RemoteAddr().String(), err, string(buf))
		}
	}()
	var (
		command CommandProto
	)
	err = ErrPanic
	switch wrap.Command {
	case message.Type_Redis_Set:
		command = NewRedisSet(wrap)
	case message.Type_Redis_Get:
		command = NewRedisGet(wrap)
	case message.Type_Redis_Del:
		command = NewRedisDel(wrap)
	case message.Type_Redis_Exists:
		command = NewRedisExists(wrap)
	case message.Type_Redis_Hset:
		command = NewRedisHSet(wrap)
	case message.Type_Redis_Hget:
		command = NewRedisHGet(wrap)
	case message.Type_Redis_Hmset:
		command = NewRedisHmSet(wrap)
	case message.Type_Redis_Hmget:
		command = NewRedisHmGet(wrap)
	case message.Type_Redis_Hexists:
		command = NewRedisHExists(wrap)
	default:
		err = fmt.Errorf("invalid redis command")
		return
	}
	//command.Print()
	if err = command.Check(); err == nil {
		lines, err = command.BSave()
	}
	return
}
