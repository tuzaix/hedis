// Autogenerated by Thrift Compiler (0.9.1)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package hbase_thrift

//import (
//	"defined"
//	"fmt"
//	tf "git.apache.org/thrift.git/lib/go/thrift"
//	"net"
//	"os"
//)
//
//var (
//	client *THBaseServiceClient
//)
//
//func init() {
//	if client == nil {
//
//		trans, err := tf.NewTSocket(net.JoinHostPort("bhd02", "9090"))
//		fmt.Println(err)
//		protocolFactory := tf.NewTBinaryProtocolFactoryDefault()
//		client = NewTHBaseServiceClientFactory(trans, protocolFactory)
//		if err := trans.Open(); err != nil {
//			fmt.Println(err)
//			os.Exit(1)
//		}
//	}
//}
//
//func HBPut(mc *defined.MemacacheCmd) {
//	tput := NewTPut()
//	tput.Row = []byte(mc.Key)
//	tput.ColumnValues = []*TColumnValue{&TColumnValue{Family: []byte("fa"), Qualifier: []byte("f1"), Value: mc.Bytes}}
//	fmt.Println(tput)
//
//	client.Put([]byte("test:haha"), tput)
//}
