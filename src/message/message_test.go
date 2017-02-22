package message

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestReadMessage(t *testing.T) {

	//var (
	//	buf *bytes.Buffer = bytes.NewBufferString("*3\r\n$3\r\nset\r\n$55\r\nhbase_namespace:hbase_tablename:hbase_column_family|key\r\n$5\r\nvalue\r\n") //set
	//	//buf *bytes.Buffer = bytes.NewBufferString("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n") //get
	//	//buf *bytes.Buffer = bytes.NewBufferString("*2\r\n$3\r\ndel\r\n$3\r\nkey\r\n") //del
	//	//buf *bytes.Buffer = bytes.NewBufferString("*2\r\n$6\r\nexists\r\n$3\r\nkey\r\n") //exists
	//	//buf *bytes.Buffer = bytes.NewBufferString("*3\r\n$4\r\nhget\r\n$3\r\nkey\r\n$5\r\nfield\r\n") //hget
	//	//buf *bytes.Buffer = bytes.NewBufferString("*4\r\n$4\r\nhset\r\n$3\r\nkey\r\n$5\r\nfield\r\n$3\r\nfoo\r\n") //hset
	//	//buf *bytes.Buffer = bytes.NewBufferString("*3\r\n$7\r\nhexists\r\n$3\r\nkey\r\n$5\r\nfield\r\n") //hexists
	//	//buf *bytes.Buffer = bytes.NewBufferString("*4\r\n$5\r\nhmget\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n") //hmget
	//	//buf *bytes.Buffer = bytes.NewBufferString("*6\r\n$5\r\nhmset\r\n$3\r\nkey\r\n$6\r\nfield1\r\n$5\r\nHello\r\n$6\r\nfield2\r\n$5\r\nWorld\r\n") //hmset
	//	br  *bufio.Reader = bufio.NewReader(buf)
	//	msg *Message       = NewMessage()
	//	err error
	//)
	//err = msg.ReadOne(br)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//if len(msg.BytesArrays) != int(msg.Len) {
	//	t.Fatal(fmt.Errorf("parsed paramter not equal"))
	//}

	var (
		//buf *bytes.Buffer = bytes.NewBufferString("*3\r\n$3\r\nset\r\n$55\r\nhbase_namespace:hbase_tablename:hbase_column_family|key\r\n$5\r\nvalue\r\n") //set
		//buf *bytes.Buffer = bytes.NewBufferString("*3\r\n$3\r\nset\r\n$23\r\nnamespace:tablename|key\r\n$5\r\nvalue\r\n") //set
		buf *bytes.Buffer = bytes.NewBufferString("*6\r\n$5\r\nhmset\r\n$55\r\nhbase_namespace:hbase_tablename:hbase_column_family|key\r\n$6\r\nfield1\r\n$5\r\nHello\r\n$6\r\nfield2\r\n$5\r\nWorld\r\n") //set
		br  *bufio.Reader = bufio.NewReader(buf)
		msg *Message      = NewMessage()
		err error
	)
	err = msg.ReadOne(br)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg.BytesArrays) != int(msg.Len) {
		t.Fatal(fmt.Errorf("parsed paramter not equal"))
	}

	hwm := NewHedisWrapMessage()
	fmt.Println(hwm.Wrap(msg))
	fmt.Println("namespace:", string(hwm.HbaseNameSpace), "table:", string(hwm.Table), "column:", string(hwm.ColumnFamily), "key:", string(hwm.Key))

	for i, b := range hwm.ExtData {
		fmt.Println(i, string(b))
	}

}
