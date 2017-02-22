package hbase

import (
	"common"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	HConfig       *common.HedisConfig
	HbaseConnPool HbaseClientPool
)

type HbaseClientPool struct {
	sync.Mutex
	GetHbaseConnTimeout time.Duration
	Pool                []*HedisHbaseConn // hbase 连接池
	PoolIndex           chan int
}

func InitHbaseConnPool(hc *common.HedisConfig) {
	HConfig = hc
	size := hc.HedisCFG.HedisHbasePoolSize

	if size <= 0 {
		common.Infof("hbase client pool size is lt zero")
		return
	}
	HbaseConnPool = HbaseClientPool{
		GetHbaseConnTimeout: time.Duration(time.Duration(hc.HedisCFG.GetHbaseConnTimeout) * time.Millisecond),
		Pool:                make([]*HedisHbaseConn, 0),
		PoolIndex:           make(chan int, size),
	}
	var (
		cli *HedisHbaseConn
		err error
	)
	for i := 0; i < hc.HedisCFG.HedisHbasePoolSize; i++ {
		cli, err = NewHedisHbaseConn(hc)
		if err != nil {
			panic("create hbase client connect failure " + err.Error())
		}
		HbaseConnPool.Pool = append(HbaseConnPool.Pool, cli)
		HbaseConnPool.ReleaseConn(i)
		common.Infof("create hbase client num: %v conn %v sucess", i, cli.HbaseTrans.Conn().LocalAddr().String())
	}

	// 使用信号,控制退出,死循环为了避免异常退出导致checkloop中断
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGUSR2)
	go func() {
		for {
			select {
			case <-c:
				return
			default:
				HbaseConnPool.CheckLoop()
			}
		}
	}()
}

func (this *HbaseClientPool) CheckLoop() {
	defer func() {
		if err := recover(); err != nil {
			common.Errorf("hbase thrift pool check loop exit %v", err)
		}
	}()
	var (
		i             int
		err           error
		cli           *HedisHbaseConn
		preConnString string
	)
	for range time.Tick(time.Duration(HConfig.HbaseThrift.HbaseKeepaliveTimeout) * time.Millisecond) {
		for i, cli = range HbaseConnPool.Pool {
			common.Debugf("check num %d connection %v", i, cli.HbaseTrans.Conn().LocalAddr().String())
			if !cli.CheckConnAlive() {
				preConnString = cli.HbaseTrans.Conn().LocalAddr().String()
				err = cli.ReCreateConn()
				if err != nil {
					common.Errorf("re create conn failure %v", err.Error())
					continue
				}
				common.Infof("num %d, conn %v is terminal, recreate %v", i, preConnString, cli.HbaseTrans.Conn().LocalAddr().String())
			}
		}
	}
}

func (this *HbaseClientPool) GetConn() (connIndex int, hhc *HedisHbaseConn, err error) {
	select {
	case connIndex = <-this.PoolIndex:
		hhc = this.Pool[connIndex]
		return
	case <-time.After(this.GetHbaseConnTimeout):
		return -1, nil, ErrGetConnTimeout
	}
}

func (this *HbaseClientPool) ReleaseConn(index int) {
	//this.Lock()
	//defer this.Unlock()
	this.PoolIndex <- index
}

// ---------------------------------------------------------------------------------------
func Set(namespace, table, family, key, value []byte) (err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.Set(namespace, table, family, key, value)
}

func HMSet(namespace, table, family, key []byte, columnValueMap map[string][]byte) (err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.HMSet(namespace, table, family, key, columnValueMap)
}

func HMGet(namespace, table, family, key []byte, columns [][]byte) (rvalue map[string][]byte, err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.HMGet(namespace, table, family, key, columns)
}

func Get(namespace, table, family, key []byte) (value []byte, err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.Get(namespace, table, family, key)

}

func Del(namespace, table, family, key []byte) (err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.Del(namespace, table, family, key)
}

func DelColumns(namespace, table, family, key []byte, columns [][]byte) (err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.DelColumns(namespace, table, family, key, columns)
}

func Exists(namespace, table, family, key []byte) (ok bool, err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.Exists(namespace, table, family, key)
}

func HExists(namespace, table, family, key, column []byte) (ok bool, err error) {
	index, cli, err := HbaseConnPool.GetConn()
	if err != nil {
		return
	}
	defer HbaseConnPool.ReleaseConn(index)
	return cli.ExistsColumns(namespace, table, family, key, column)
}
