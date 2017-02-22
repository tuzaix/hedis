package hbase

import (
	"bytes"
	"common"
	"fmt"
	tf "git.apache.org/thrift.git/lib/go/thrift"
	ht "hbase_thrift"
	"net"
	"sync"
)

var (
	NamespaceAndTableDelim = []byte{byte(':')}
	Ping                   = []byte("ping-u12OOUyDQKFXBDjd")
)

// hbase client 链接
type HedisHbaseConn struct {
	sync.Mutex
	HbaseClient *ht.THBaseServiceClient
	HbaseTrans  *tf.TSocket

	HbaseHedis     common.HbaseHedisDefault
	HedisConfig    *common.HedisConfig
	ConnectTimeout int
	ReadTimeout    int
	WriteTimeout   int
}

func NewHedisHbaseConn(hc *common.HedisConfig) (hhc *HedisHbaseConn, err error) {
	var (
		cli   *ht.THBaseServiceClient
		trans *tf.TSocket
	)
	cli, trans, err = createConn(hc)
	if err != nil {
		return
	}
	hhc = &HedisHbaseConn{
		HbaseClient:    cli,
		HbaseTrans:     trans,
		HedisConfig:    hc,
		HbaseHedis:     hc.HbaseHedisDefault,
		ConnectTimeout: hc.HbaseThrift.HbaseConnectTimeout,
		ReadTimeout:    hc.HbaseThrift.HbaseReadTimeout,
		WriteTimeout:   hc.HbaseThrift.HbaseWriteTimeout,
	}
	return hhc, nil
}

func createConn(hc *common.HedisConfig) (cli *ht.THBaseServiceClient, trans *tf.TSocket, err error) {
	var (
		proto *tf.TBinaryProtocolFactory
	)
	if trans, err = tf.NewTSocket(net.JoinHostPort(hc.HbaseThrift.HbaseHost,
		fmt.Sprintf("%d", hc.HbaseThrift.HbasePort))); err != nil {
		return
	}
	proto = tf.NewTBinaryProtocolFactoryDefault()
	cli = ht.NewTHBaseServiceClientFactory(trans, proto)
	if err = trans.Open(); err != nil {
		return
	}
	return
}

func (this *HedisHbaseConn) ReCreateConn() (err error) {
	this.Lock()
	defer this.Unlock()
	var (
		cli   *ht.THBaseServiceClient
		trans *tf.TSocket
	)
	cli, trans, err = createConn(this.HedisConfig)
	if err != nil {
		return
	}
	this.HbaseClient = cli
	this.HbaseTrans = trans
	return
}

func (this *HedisHbaseConn) CheckConnAlive() (ok bool) {
	defer func() {
		if err := recover(); err != nil {
			common.Errorf("check self conn %v", err)
		}
	}()
	var err error
	if ok, err = this.Exists(this.HbaseHedis.Namespace, this.HbaseHedis.Table, Ping, Ping); !ok {
		return true
	} else if err != nil {
		return false
	}
	common.Errorf("check alive err %v, %v", ok, err)
	return false
}

func (this *HedisHbaseConn) formatSet(namespace, table, family []byte) (namespaceWithTable, s_family []byte) {
	var (
		s_namespace, s_table []byte
	)
	if len(table) == 0 {
		s_table = this.HbaseHedis.Table
	} else {
		s_table = table
	}
	if len(namespace) == 0 {
		s_namespace = this.HbaseHedis.Namespace
	} else {
		s_namespace = namespace
	}
	if len(family) == 0 {
		s_family = this.HbaseHedis.Family
	} else {
		s_family = family
	}
	namespaceWithTable = bytes.Join([][]byte{s_namespace, s_table}, NamespaceAndTableDelim)
	return
}

func (this *HedisHbaseConn) Set(namespace, table, family, key, value []byte) error {
	return this.HMSet(namespace, table, family, key, map[string][]byte{this.HbaseHedis.SDefaultColumn: value})
}

func (this *HedisHbaseConn) HMSet(namespace, table, family, key []byte, columnValueMap map[string][]byte) error {
	this.Lock()
	defer this.Unlock()
	if len(columnValueMap) <= 0 {
		return fmt.Errorf("not column set")
	}
	var (
		tput               *ht.TPut
		k                  string
		v                  []byte
		namespaceWithTable []byte
		tv                 *ht.TColumnValue
		tvs                []*ht.TColumnValue
		index              int = 0
	)
	namespaceWithTable, family = this.formatSet(namespace, table, family)
	tvs = make([]*ht.TColumnValue, len(columnValueMap))
	for k, v = range columnValueMap {
		tv = &ht.TColumnValue{
			Family:    family,
			Qualifier: []byte(k),
			Value:     v,
		}
		tvs[index] = tv
		index++
	}
	tput = &ht.TPut{
		Row:          key,
		ColumnValues: tvs,
	}
	if len(namespaceWithTable) != 0 {
		return this.HbaseClient.Put(namespaceWithTable, tput)
	}
	return ErrNotSpecailTable
}

func (this *HedisHbaseConn) Get(namespace, table, family, key []byte) (value []byte, err error) {
	var (
		ok     bool
		rvalue map[string][]byte
	)
	rvalue, err = this.HMGet(namespace, table, family, key, [][]byte{this.HbaseHedis.DefaultColumn})
	if len(rvalue) > 0 {
		if value, ok = rvalue[this.HbaseHedis.SDefaultColumn]; ok {
			return value, nil
		}
	}
	return nil, ErrNullValue
}

func (this *HedisHbaseConn) HMGet(namespace, table, family, key []byte, columns [][]byte) (rvalue map[string][]byte, err error) {
	this.Lock()
	defer this.Unlock()
	var (
		tget               *ht.TGet
		tcs                []*ht.TColumn
		tc                 *ht.TColumn
		column             []byte
		index              int
		tresult            *ht.TResult_
		namespaceWithTable []byte
	)
	namespaceWithTable, family = this.formatSet(namespace, table, family)
	if columns != nil && len(columns) > 0 {
		tcs = make([]*ht.TColumn, len(columns))

		for index, column = range columns {
			tc = &ht.TColumn{
				Family:    family,
				Qualifier: column,
			}
			tcs[index] = tc
		}
	}
	tget = &ht.TGet{
		Row:     key,
		Columns: tcs,
	}
	if len(namespaceWithTable) != 0 {
		if tresult, err = this.HbaseClient.Get(namespaceWithTable, tget); err == nil {
			if len(tresult.ColumnValues) > 0 {
				rvalue = make(map[string][]byte)
				for _, tv := range tresult.ColumnValues {
					rvalue[string(tv.Qualifier)] = tv.Value
				}
			} else {
				err = ErrNullValue
			}
		} else {
			common.Errorf("%v %v", this.HbaseTrans.Conn().RemoteAddr(), err.Error())
		}

	} else {
		err = ErrNotSpecailTable
	}
	return
}

func (this *HedisHbaseConn) Exists(namespace, table, family, key []byte) (ok bool, err error) {
	return this.ExistsColumns(namespace, table, family, key, nil)
}

func (this *HedisHbaseConn) ExistsColumns(namespace, table, family, key, column []byte) (ok bool, err error) {
	this.Lock()
	defer this.Unlock()
	var (
		tget               *ht.TGet
		namespaceWithTable []byte
	)
	namespaceWithTable, family = this.formatSet(namespace, table, family)
	if column != nil || len(column) > 0 {
		tget = &ht.TGet{
			Row:     key,
			Columns: []*ht.TColumn{&ht.TColumn{Family: family, Qualifier: column}},
		}
	} else {
		tget = &ht.TGet{
			Row: key,
		}
	}
	if len(namespaceWithTable) != 0 {
		ok, err = this.HbaseClient.Exists(namespaceWithTable, tget)
	} else {
		ok = false
		err = ErrNotSpecailTable
	}
	return
}

func (this *HedisHbaseConn) Del(namespace, table, family, key []byte) error {
	return this.DelColumns(namespace, table, family, key, nil)
}

func (this *HedisHbaseConn) DelColumns(namespace, table, family, key []byte, columns [][]byte) (err error) {
	this.Lock()
	defer this.Unlock()

	var (
		tdelete            *ht.TDelete
		tcolumns           []*ht.TColumn
		index              int
		column             []byte
		namespaceWithTable []byte
	)

	namespaceWithTable, family = this.formatSet(namespace, table, family)
	if len(columns) > 0 {
		tcolumns = make([]*ht.TColumn, len(columns))
		for index, column = range columns {
			tcolumns[index] = &ht.TColumn{Family: family, Qualifier: column}
		}
	}

	tdelete = &ht.TDelete{
		Row:     key,
		Columns: tcolumns,
	}
	if len(namespaceWithTable) != 0 {
		err = this.HbaseClient.DeleteSingle(namespaceWithTable, tdelete)
	} else {
		err = ErrNotSpecailTable
	}
	return
}
