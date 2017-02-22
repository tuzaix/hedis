package hconn

import (
	"common/util"
	"time"
)

var connMap *util.SafeMap = util.NewSafeMap()

func init() {
	go func() {
		go func() {
			for range time.Tick(time.Minute) {
				checkIdleTimeout()
			}
		}()
	}()
}

func addConn(addr string, carrierConn *HedisConn) {
	connMap.Set(addr, carrierConn)
}

func removeConn(addr string) {
	connMap.Erase(addr)
}

func getConn(addr string) (carrierConn *HedisConn, ok bool) {
	var value interface{}
	if value, ok = connMap.Get(addr); !ok {
		return nil, ok
	}
	if carrierConn, ok = value.(*HedisConn); !ok {
		return nil, ok
	}
	return
}

func getBroadcastConn() (carrierConnList []*HedisConn) {
	var (
		dict        map[string]interface{} = connMap.Clone()
		value       interface{}
		carrierConn *HedisConn
		ok          bool
	)
	carrierConnList = make([]*HedisConn, 0, len(dict))
	for _, value = range dict {
		if carrierConn, ok = value.(*HedisConn); !ok {
			continue
		}
		carrierConnList = append(carrierConnList, carrierConn)
	}
	return
}

func checkIdleTimeout() {

}
