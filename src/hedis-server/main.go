package main

import (
	"common"
	"flag"
	"fmt"
	"hbase"
	"net/http"
	_ "net/http/pprof"
)

var (
	conf = flag.String("conf", "../conf/hedis.conf", "hedis toml config")
)

func main() {
	flag.Parse()

	// 加载配置
	common.LoadConfig(*conf)

	// 启动pprof
	go func() {
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", common.Config.HedisCFG.HedisPprofPort), nil)
	}()

	//// 初始化日志
	common.InitLogger(common.Config.HedisLOG)

	// 初始化服务与hbase的连接池
	hbase.InitHbaseConnPool(common.Config)
	//
	// 启动 服务监听
	StartHedisServer(common.Config)
}
