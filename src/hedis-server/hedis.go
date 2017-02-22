package main

import (
	"common"
	"fmt"
	"hconn"
	"net"
	"os"
	"time"
)

func StartHedisServer(hc *common.HedisConfig) {
	listener, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", hc.HedisCFG.HedisServerPort))
	if err != nil {
		common.Errorf("error listening: %s", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	common.Infof("start hedis server @ %v", hc.HedisCFG.HedisServerPort)
	var (
		tcpConn *net.TCPConn
		ok      bool
	)
	for {
		conn, err := listener.Accept()
		if err != nil {
			common.Errorf("Error accept: %s", err.Error())
			continue
		}
		// 设置keepalive 心跳检查
		if tcpConn, ok = conn.(*net.TCPConn); ok {
			common.Debugf("set keepalive %v", conn.RemoteAddr().String())
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(time.Duration(hc.HedisCFG.HedisKeepAliveTimeout) * time.Millisecond)
		}
		common.Infof("client from %v", conn.RemoteAddr().String())
		go func(conn net.Conn, hc *common.HedisConfig) {
			defer func() {
				common.Infof("client terminal connection %v", conn.RemoteAddr().String())
				conn.Close()
			}()
			hconn.ClientRun(conn, hc)
		}(conn, hc)
	}
}
