package message

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

const (
	SimpleStringsPrefix byte = byte('+')
	ErrorsStringsPrefix byte = byte('-')
	IntegersPrefix      byte = byte(':')
	BulkStringsPrefix   byte = byte('$')
	ArraysPrefix        byte = byte('*')
	CrSuffix            byte = byte('\r')
	LfSuffix            byte = byte('\n')
)

var (
	CrlfSuffix     []byte = []byte{CrSuffix, LfSuffix}
	ParseCRLFError error  = fmt.Errorf("parse crlf error")
)

type Message struct {
	Len            int64
	BytesArrays    [][]byte
	rawBytesBuffer *bytes.Buffer
}

func NewMessage() *Message {
	return &Message{rawBytesBuffer: new(bytes.Buffer)}
}

func (this *Message) ReadOne(bReader *bufio.Reader) (err error) {
	var prefix []byte
	if prefix, err = bReader.Peek(1); err != nil {
		return
	}
	switch prefix[0] {
	case BulkStringsPrefix:
		return this.readBulkStrings(bReader)
	case ArraysPrefix:
		return this.readArrays(bReader)
	default:
		return fmt.Errorf("illegal first byte: %s", prefix)
	}
}

func (this *Message) readBulkStrings(bReader *bufio.Reader) (err error) {
	var (
		bytesValue    []byte
		integersValue int64
	)
	if bytesValue, err = bReader.ReadBytes(LfSuffix); err != nil {
		return
	}
	if !bytes.HasSuffix(bytesValue, CrlfSuffix) {
		return ParseCRLFError
	}
	if integersValue, err = strconv.ParseInt(string(bytesValue[1:len(bytesValue)-2]), 10, 64); err != nil {
		return
	}
	if integersValue < 0 {
		return
	}
	var (
		bufferBytesValue []byte
		readNBytes       int
	)
	bytesValue = make([]byte, integersValue+2)
	for bufferBytesValue = bytesValue; len(bufferBytesValue) > 0; bufferBytesValue = bufferBytesValue[readNBytes:] {
		if readNBytes, err = bReader.Read(bufferBytesValue); err != nil {
			return
		}
	}
	if !bytes.HasSuffix(bytesValue, CrlfSuffix) {
		return ParseCRLFError
	}
	if err = binary.Write(this.rawBytesBuffer, binary.BigEndian, bytesValue[:integersValue]); err != nil {
		return
	}
	return
}

func (this *Message) readArrays(br *bufio.Reader) (err error) {

	var (
		bytesValue    []byte
		integersValue int64
	)
	if bytesValue, err = br.ReadBytes(LfSuffix); err != nil {
		return
	}
	if !bytes.HasSuffix(bytesValue, CrlfSuffix) {
		return ParseCRLFError
	}
	if integersValue, err = strconv.ParseInt(string(bytesValue[1:len(bytesValue)-2]), 10, 64); err != nil {
		return
	}
	this.Len = integersValue
	if integersValue < 0 {
		return
	}

	var (
		elementIndex int64
		elementValue *Message
	)

	this.BytesArrays = make([][]byte, this.Len)

	for elementIndex = 0; elementIndex < integersValue; elementIndex++ {
		elementValue = NewMessage()
		if err = elementValue.ReadOne(br); err != nil {
			return
		}
		if bytesValue = elementValue.rawBytesBuffer.Bytes(); len(bytesValue) == 0 {
			return
		}
		this.BytesArrays[elementIndex] = bytesValue
	}
	return
}
