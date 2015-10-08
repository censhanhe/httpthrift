package httpthrift

import (
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/dt/go-metrics-reporting"
)

// Thrift's generated Processors have `GetProcessorFunction` and satisfy this interface.
type HasProcessFunc interface {
	GetProcessorFunction(key string) (processor thrift.TProcessorFunction, ok bool)
}

// Wraps a generated thrift Processor, providing a ServeHTTP method to serve thrift-over-http.
type ThriftOverHTTPHandler struct {
	stats *report.Recorder
	HasProcessFunc
}

func NewThriftOverHTTPHandler(p HasProcessFunc, stats *report.Recorder) *ThriftOverHTTPHandler {
	return &ThriftOverHTTPHandler{stats, p}
}

// Mostly borrowed from generated thrift code `Process` method, but with timing added.
func (p ThriftOverHTTPHandler) handle(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}

	if processor, ok := p.GetProcessorFunction(name); ok {
		start := time.Now()
		success, err = processor.Process(seqId, iprot, oprot)
		if p.stats != nil {
			p.stats.TimeSince(name, start)
		}
		return
	}

	iprot.Skip(thrift.STRUCT)
	iprot.ReadMessageEnd()
	e := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)

	oprot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
	e.Write(oprot)
	oprot.WriteMessageEnd()
	oprot.Flush()

	return false, e
}

func (h ThriftOverHTTPHandler) ServeHTTP(out http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		var in *thrift.TMemoryBuffer
		size := int(req.ContentLength)
		if size > 0 {
			in = thrift.NewTMemoryBufferLen(size)
		} else {
			in = thrift.NewTMemoryBuffer()
		}

		in.ReadFrom(req.Body)
		defer req.Body.Close()

		compact := false

		if in.Len() > 0 && in.Bytes()[0] == thrift.COMPACT_PROTOCOL_ID {
			compact = true
		}

		outbuf := thrift.NewTMemoryBuffer()

		var iprot thrift.TProtocol
		var oprot thrift.TProtocol

		if compact {
			iprot = thrift.NewTCompactProtocol(in)
			oprot = thrift.NewTCompactProtocol(outbuf)
		} else {
			iprot = thrift.NewTBinaryProtocol(in, true, true)
			oprot = thrift.NewTBinaryProtocol(outbuf, true, true)
		}

		ok, err := h.handle(iprot, oprot)

		if ok {
			outbuf.WriteTo(out)
		} else {
			http.Error(out, err.Error(), 500)
		}
	} else {
		http.Error(out, "Must POST TBinary encoded thrift RPC", 401)
	}
}
