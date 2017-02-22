package hconn

import (
	"bufio"
	"common"
	"common/util"
	"hproto"
	"io"
	"message"
	"net"
	"strings"
	"time"
)

var (
	UnknownErr = []byte("-Error messa\r\n")
)

type HedisConn struct {
	HConn   net.Conn
	BReader *bufio.Reader
	BWriter *bufio.Writer

	// 客户端与hedis的相关超时
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration

	latestActiveTime time.Time
	closed           *util.SyncClose
}

func ClientRun(conn net.Conn, hc *common.HedisConfig) {
	var hdc *HedisConn
	hdc = &HedisConn{
		HConn:          conn,
		BReader:        bufio.NewReader(conn),
		BWriter:        bufio.NewWriter(conn),
		ConnectTimeout: time.Duration(hc.HedisCFG.HedisConnectTimeout) * time.Millisecond,
		ReadTimeout:    time.Duration(hc.HedisCFG.HedisReadTimeout) * time.Millisecond,
		WriteTimeout:   time.Duration(hc.HedisCFG.HedisWriteTimeout) * time.Millisecond,
		IdleTimeout:    time.Duration(hc.HedisCFG.HedisIdleTimeout) * time.Millisecond,

		latestActiveTime: time.Now(),
		closed:           util.NewSyncClose(),
	}

	go hdc.checkConnIdle()
	hdc.handleLoop()
}

func (this *HedisConn) checkConnIdle() {
	ticker := time.NewTicker(1 * time.Second) // 文件切割
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// 超时检查
			if this.latestActiveTime.Before(time.Now().Add(-this.IdleTimeout)) {
				common.Infof("client idle timeout %v", this.HConn.RemoteAddr().String())
				this.Close()
				return
			}
		case <-this.closed.IsClosed():
			common.Infof("exit %v check conn idle loop", this.HConn.RemoteAddr().String())
			return
		}
	}
}

// wrapErr
func (this *HedisConn) wrapErr(err error) []string {
	return []string{"-ERR ", err.Error(), "\r\n"}
}

// 读取stream 内容的 routine
func (this *HedisConn) handleLoop() {
	var (
		wrapMsg *message.HedisWrapMessage
		msg     *message.Message
		err     error
		lines   []string
	)
	for {
		select {
		case <-this.closed.IsClosed():
			common.Infof("exit %v handle loop", this.HConn.RemoteAddr().String())
			return
		default:
			msg = message.NewMessage()
			if err = msg.ReadOne(this.BReader); err == nil {
				wrapMsg = message.NewHedisWrapMessage()
				if err = wrapMsg.Wrap(msg); err == nil {
					if lines, err = hproto.Exec(wrapMsg, this.HConn); err != nil {
						this.Write(this.wrapErr(err))
					} else {
						this.Write(lines)
					}
				} else {
					this.Write(this.wrapErr(err))
				}
				this.latestActiveTime = time.Now()
			} else if err == io.EOF {
				this.Close()
				common.Infof("client is terminal %v in readLoop", this.HConn.RemoteAddr().String())
				return
			}
		}
	}
}

func (this *HedisConn) Write(lines []string) (err error) {
	var (
		buf []byte
		nn  int
	)
	err = this.HConn.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
	if err != nil {
		return
	}
	if lines == nil || len(lines) == 0 {
		buf = UnknownErr
	} else {
		buf = []byte(strings.Join(lines, ""))
	}
	if nn, err = this.BWriter.Write(buf); err != nil || nn != len(buf) {
		return err
	}
	return this.BWriter.Flush()
}

func (this *HedisConn) Close() {
	this.HConn.Close()
	this.closed.Close()
}
