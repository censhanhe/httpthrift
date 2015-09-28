package httpthrift

import (
	"io"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
)

type sendProt struct {
	transport *http.Client
	url       func() string
	sendbuf   *thrift.TMemoryBuffer
	recvbuf   *thrift.TMemoryBuffer

	*thrift.TBinaryProtocol
}

func (t *sendProt) Flush() error {
	req, err := http.NewRequest("POST", t.url(), t.sendbuf)
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

func getSendProt(url func() string, recvbuf *thrift.TMemoryBuffer) thrift.TProtocol {
	sendbuf := thrift.NewTMemoryBuffer()
	underlying := thrift.NewTBinaryProtocol(sendbuf, true, true)
	return &sendProt{&http.Client{}, url, sendbuf, recvbuf, underlying}
}

func NewDynamicClientProts(url func() string) (recv, send thrift.TProtocol) {
	recvbuf := thrift.NewTMemoryBuffer()
	send = getSendProt(url, recvbuf)
	recv = thrift.NewTBinaryProtocol(recvbuf, true, true)
	return recv, send
}

// pass these to the generated `NewFooClientProtocol(nil, recv, send)` method.
func NewClientProts(url string) (recv, send thrift.TProtocol) {
	return NewDynamicClientProts(func() string { return url })
}
