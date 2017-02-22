package common

import (
	"github.com/BurntSushi/toml"
	"io/ioutil"
)

var (
	Config *HedisConfig
)

type HedisConfig struct {
	HedisCFG          HedisServerConfig `toml:"hedis_server"` // hedis server 配置
	HedisLOG          HedisLOGConfig    `toml:"hedis_log"`    // heids log 配置
	HbaseThrift       HbaseThriftConfig `toml:"hbase_thrift"` // hbase 配置
	HbaseHedisDefault HbaseHedisDefault `toml:"hbase_default_table"`
}

type HbaseHedisDefault struct {
	SNamespace     string `toml:"default_namespace"`
	STable         string `toml:"default_table"`
	SFamily        string `toml:"default_family"`
	SDefaultColumn string `toml:"default_column"` // 默认的column 名称, 用于单一 set()

	Namespace     []byte
	Table         []byte
	Family        []byte
	DefaultColumn []byte
}

type HedisServerConfig struct {
	HedisServerPort     int `toml:"server_port"`
	HedisPprofPort      int `toml:"pprof_port"`
	HedisRestPort       int `toml:"rest_port"`
	HedisHbasePoolSize  int `toml:"hbase_pool_size"`
	GetHbaseConnTimeout int `toml:"get_hbase_conn_timeout"`

	HedisKeepAliveTimeout int `toml:keepalive_timeout`
	HedisConnectTimeout   int `toml:"connect_timeout"`
	HedisReadTimeout      int `toml:"read_timeout"`
	HedisWriteTimeout     int `toml:"write_timeout"`
	HedisIdleTimeout      int `toml:"idle_timeout"`
}

type HedisLOGConfig struct {
	LogLevel     string `toml:"level"`
	LogConsole   int    `toml:"console"`
	LogDir       string `toml:"dir"`
	LogFilename  string `toml:"filename"`
	LogCount     int    `toml:"count"`
	LogSuffix    string `toml:"suffix"`
	LogColorfull int    `toml:"colorfull"`
}

type HbaseThriftConfig struct {
	HbaseHost             string `toml:"host"`
	HbasePort             int    `toml:"port"`
	HbaseConnectTimeout   int    `toml:"connect_timeout"`
	HbaseReadTimeout      int    `toml:"read_timeout"`
	HbaseWriteTimeout     int    `toml:"write_timeout"`
	HbaseKeepaliveTimeout int    `toml:"keepalive_timeout"`
}

func reInitHedisConfig(hc *HedisConfig) {
	hc.HbaseHedisDefault.Namespace = []byte(hc.HbaseHedisDefault.SNamespace)
	hc.HbaseHedisDefault.Family = []byte(hc.HbaseHedisDefault.SFamily)
	hc.HbaseHedisDefault.Table = []byte(hc.HbaseHedisDefault.STable)
	hc.HbaseHedisDefault.DefaultColumn = []byte(hc.HbaseHedisDefault.SDefaultColumn)
}

// 初始化全局配置文件
func LoadConfig(filename string) {
	var (
		data []byte
		err  error
	)

	data, err = ioutil.ReadFile(filename)

	if err != nil {
		panic("read configuration file failed " + err.Error())
	}

	var config HedisConfig
	if _, err = toml.Decode(string(data), &config); err != nil {
		panic("toml decode failed " + err.Error())
	}
	Config = &config
	reInitHedisConfig(Config)
	return
}
