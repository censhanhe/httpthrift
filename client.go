package httpthrift

import (
	"io"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
)

type ThriftOverHttpSendProt struct {
	transport *http.Client
	url       string
	sendbuf   *thrift.TMemoryBuffer
	recvbuf   *thrift.TMemoryBuffer

	*thrift.TBinaryProtocol
}

func (t *ThriftOverHttpSendProt) Flush() error {
	req, err := http.NewRequest("POST", t.url, t.sendbuf)
	req.Header.Set("Content-Length", string(t.sendbuf.Len()))
	req.Header.Set("Content-Type", "application/x-thrift")

	resp, err := t.transport.Do(req)
	if err != nil {
		return err
	}

	io.Copy(t.recvbuf, resp.Body)
	resp.Body.Close()
	return nil
}

func NewThriftOverHttpSendProt(url string, recvbuf *thrift.TMemoryBuffer) thrift.TProtocol {
	sendbuf := thrift.NewTMemoryBuffer()
	underlying := thrift.NewTBinaryProtocol(sendbuf, true, true)
	return &ThriftOverHttpSendProt{&http.Client{}, url, sendbuf, recvbuf, underlying}
}

func NewThriftHttpRpcClient(url string, client func(in, out thrift.TProtocol) interface{}) interface{} {
	recvbuf := thrift.NewTMemoryBuffer()
	send := NewThriftOverHttpSendProt(url, recvbuf)
	recv := thrift.NewTBinaryProtocol(recvbuf, true, true)
	return client(recv, send)
}
