package hbase

import "errors"

var (
	ErrNullValue       = errors.New("none value")
	ErrNotSpecailTable = errors.New("not specify hbase namespace and table")
	ErrGetConnTimeout  = errors.New("get hbase conn timeout")
)
