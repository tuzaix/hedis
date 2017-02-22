package hbase

import (
	"common"
	"fmt"
	"testing"
)

func TestInitHbaseClientPool(t *testing.T) {
	htc := common.HbaseThriftConfig{
		HbaseHost:           "bhd01",
		HbasePort:           19090,
		HbaseConnectTimeout: 1000,
	}
	hc := &common.HedisConfig{
		HedisCFG:          common.HedisServerConfig{HedisHbasePoolSize: 2},
		HbaseThrift:       htc,
		HbaseHedisDefault: common.HbaseHedisDefault{SNamespace: "hedis", STable: "redis", SFamily: "fa", SDefaultColumn: "hedis"},
	}
	InitHbaseConnPool(hc)

	//cli, err := HbaseConnPool.GetConn()
	//if err != nil {
	//	panic(err.Error())
	//}
	//table := []byte("redis")
	//for i := 0; i < 10; i++ {
	//	key := []byte(fmt.Sprintf("row_key%d", i))
	//	value := []byte(fmt.Sprintf("row_value%d", i))
	//	fmt.Println(cli.Set([]byte("hedis"), table, []byte("fa"), key, value))
	//	//line, err := cli.Get(table, key)
	//	//fmt.Println(string(line), err)
	//	//fmt.Println(cli.Exists(table, key))
	//	//cli.Del(table, key)
	//	//fmt.Println(cli.Exists(table, key))
	//}
	//HbaseConnPool.ReleaseConn(cli)
}
