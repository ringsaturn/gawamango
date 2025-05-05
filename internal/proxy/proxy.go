package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ringsaturn/gawamango/internal/protocol"
	"go.uber.org/zap"
)

// Trace represents timing information for a MongoDB request/response cycle
type Trace struct {
	RequestID         int32         // MongoDB request ID
	StartTime         time.Time     // When the request was received from client
	ClientToProxyTime time.Duration // Time spent reading from client
	ProxyToServerTime time.Duration // Time spent writing to server
	ServerProcessTime time.Duration // Time spent on server processing
	ServerToProxyTime time.Duration // Time spent reading from server
	ProxyToClientTime time.Duration // Time spent writing to client
	TotalTime         time.Duration // Total end-to-end time
	Command           string        // MongoDB command
	Database          string        // Database name
}

type ProxyOption func(*Proxy)

func WithSilent(silent bool) ProxyOption {
	return func(p *Proxy) {
		p.silent = silent
	}
}

func WithLogger(logger *zap.Logger) ProxyOption {
	return func(p *Proxy) {
		p.logger = logger
	}
}

// Proxy represents a MongoDB proxy instance
type Proxy struct {
	listenAddr string
	targetAddr string
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	silent     bool
	logger     *zap.Logger
	traces     map[int32]*Trace // Map of request ID to trace
	tracesMu   sync.RWMutex     // Mutex to protect the traces map
}

// NewProxy creates a new MongoDB proxy instance
func NewProxy(listenAddr, targetAddr string, opts ...ProxyOption) (*Proxy, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		ctx:        ctx,
		cancel:     cancel,
		traces:     make(map[int32]*Trace),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p, nil
}

// Start begins accepting connections
func (p *Proxy) Start() error {
	var err error
	p.listener, err = net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", p.listenAddr, err)
	}

	go p.acceptLoop()
	return nil
}

// Stop gracefully shuts down the proxy
func (p *Proxy) Stop() error {
	p.cancel()
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			return fmt.Errorf("error closing listener: %v", err)
		}
	}
	p.wg.Wait()
	return nil
}

func (p *Proxy) acceptLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			conn, err := p.listener.Accept()
			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				continue
			}

			p.wg.Add(1)
			go p.handleConnection(conn)
		}
	}
}

func (p *Proxy) setupLoggingMsg() func(header *protocol.MsgHeader, body []byte) {
	if p.logger == nil {
		return nil
	}

	return func(header *protocol.MsgHeader, body []byte) {
		cmd, err := protocol.ParseCommand(header.OpCode, body)
		if err != nil {
			p.logger.Error("error parsing command", zap.Error(err))
			return
		}

		p.logger.Info(
			"MongoDB Command",
			zap.String("database", cmd.Database),
			zap.String("command", cmd.CommandName),
			zap.Any("arguments", cmd.Arguments),
		)
	}
}

// addTrace creates a new trace for a request
func (p *Proxy) addTrace(header *protocol.MsgHeader, body []byte) {
	if header == nil {
		return
	}

	trace := &Trace{
		RequestID: header.RequestID,
		StartTime: time.Now(),
	}

	// Try to parse the command for more context
	cmd, err := protocol.ParseCommand(header.OpCode, body)
	if err == nil {
		trace.Command = cmd.CommandName
		trace.Database = cmd.Database
	}

	p.tracesMu.Lock()
	p.traces[header.RequestID] = trace
	p.tracesMu.Unlock()
}

// updateTrace updates timing information for a trace
func (p *Proxy) updateTrace(header *protocol.MsgHeader, stage string, duration time.Duration) {
	if header == nil {
		return
	}

	p.tracesMu.RLock()
	trace, exists := p.traces[header.ResponseTo]
	p.tracesMu.RUnlock()

	if !exists {
		return
	}

	p.tracesMu.Lock()
	defer p.tracesMu.Unlock()

	switch stage {
	case "client_to_proxy":
		trace.ClientToProxyTime = duration
	case "proxy_to_server":
		trace.ProxyToServerTime = duration
	case "server_to_proxy":
		trace.ServerToProxyTime = duration
	case "proxy_to_client":
		trace.ProxyToClientTime = duration
	}

	// Calculate server processing time
	if trace.ServerToProxyTime > 0 && trace.ProxyToServerTime > 0 {
		trace.ServerProcessTime = time.Since(trace.StartTime) -
			trace.ClientToProxyTime - trace.ProxyToServerTime -
			trace.ServerToProxyTime - trace.ProxyToClientTime
	}

	trace.TotalTime = time.Since(trace.StartTime)

	// Log the trace info
	if p.logger != nil {
		p.logger.Debug("Trace updated",
			zap.Int32("requestID", trace.RequestID),
			zap.String("command", trace.Command),
			zap.String("database", trace.Database),
			zap.String("stage", stage),
			zap.Duration("duration", duration),
			zap.Duration("totalTime", trace.TotalTime),
		)
	}
}

// completeTrace logs the final timing info and removes the trace
func (p *Proxy) completeTrace(requestID int32) {
	p.tracesMu.RLock()
	trace, exists := p.traces[requestID]
	p.tracesMu.RUnlock()

	if !exists {
		return
	}

	if p.logger != nil {
		p.logger.Debug("Request completed",
			zap.Int32("requestID", trace.RequestID),
			zap.String("command", trace.Command),
			zap.String("database", trace.Database),
			zap.Duration("clientToProxy", trace.ClientToProxyTime),
			zap.Duration("proxyToServer", trace.ProxyToServerTime),
			zap.Duration("serverProcess", trace.ServerProcessTime),
			zap.Duration("serverToProxy", trace.ServerToProxyTime),
			zap.Duration("proxyToClient", trace.ProxyToClientTime),
			zap.Duration("totalTime", trace.TotalTime),
		)
	}

	// Remove the trace
	p.tracesMu.Lock()
	delete(p.traces, requestID)
	p.tracesMu.Unlock()
}

// isBrokenPipe reports whether the I/O error is the expected result of the peer
// closing its receive side (EPIPE/ECONNRESET).  We treat these as normal
// termination signals and suppress noisy logging.
func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	if oe, ok := err.(*net.OpError); ok {
		if se, ok := oe.Err.(*os.SyscallError); ok {
			if se.Err == syscall.EPIPE || se.Err == syscall.ECONNRESET || se.Err == syscall.ENOTCONN {
				return true
			}
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "socket is not connected") ||
		strings.Contains(msg, "not connected")
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()

	// Connect to the target MongoDB server.
	serverConn, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		if err := clientConn.Close(); err != nil {
			p.logger.Error("error closing client connection", zap.Error(err))
		}
		return
	}

	// Create context with connection-specific cancelation
	connCtx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	onMessageOps := []func(header *protocol.MsgHeader, body []byte){}
	if !p.silent {
		onMessageOps = append(onMessageOps, p.setupLoggingMsg())
	}

	// Add tracing to message operations
	onMessageOps = append(onMessageOps, p.addTrace)

	onMessageFunc := func(header *protocol.MsgHeader, body []byte) {
		for _, op := range onMessageOps {
			op(header, body)
		}
	}

	// client ➜ server
	go func() {
		defer wg.Done()
		reader := &mongoReader{
			conn:      clientConn,
			onMessage: onMessageFunc,
			direction: "client_to_proxy",
			proxy:     p,
		}

		// Use an intermediate writer to track timing
		writer := &timingWriter{
			Writer:    serverConn,
			direction: "proxy_to_server",
			proxy:     p,
		}

		_, err := io.Copy(writer, reader)

		// Half‑close: signal FIN for this direction only.
		if tcp, ok := serverConn.(*net.TCPConn); ok {
			if err := tcp.CloseWrite(); err != nil && !isBrokenPipe(err) {
				p.logger.Error("error closing write on server connection", zap.Error(err))
			}
		} else {
			if err := serverConn.Close(); err != nil {
				p.logger.Error("error closing server connection", zap.Error(err))
			}
		}

		if err != nil && !isBrokenPipe(err) && connCtx.Err() == nil {
			p.logger.Error("error copying from client to server", zap.Error(err))
		}
	}()

	// server ➜ client
	go func() {
		defer wg.Done()
		reader := &mongoReader{
			conn:       serverConn,
			onMessage:  nil, // No need for onMessage for server responses
			direction:  "server_to_proxy",
			proxy:      p,
			isResponse: true,
		}

		writer := &timingWriter{
			Writer:    clientConn,
			direction: "proxy_to_client",
			proxy:     p,
		}

		_, err := io.Copy(writer, reader)

		// Half‑close: signal FIN for this direction only.
		if tcp, ok := clientConn.(*net.TCPConn); ok {
			if err := tcp.CloseWrite(); err != nil && !isBrokenPipe(err) {
				p.logger.Error("error closing write on client connection", zap.Error(err))
			}
		} else {
			if err := clientConn.Close(); err != nil {
				p.logger.Error("error closing client connection", zap.Error(err))
			}
		}

		if err != nil && !isBrokenPipe(err) && connCtx.Err() == nil {
			p.logger.Error("error copying from server to client", zap.Error(err))
		}
	}()

	// Wait for both directions to finish.
	wg.Wait()
	if err := clientConn.Close(); err != nil {
		p.logger.Error("error closing client connection", zap.Error(err))
	}
	if err := serverConn.Close(); err != nil {
		p.logger.Error("error closing server connection", zap.Error(err))
	}
}

// mongoReader 是一个自定义的io.Reader实现，用于解析MongoDB消息
type mongoReader struct {
	conn          net.Conn
	onMessage     func(header *protocol.MsgHeader, body []byte)
	buf           []byte
	currentHeader *protocol.MsgHeader
	direction     string
	proxy         *Proxy
	isResponse    bool
	readStart     time.Time
}

func (r *mongoReader) Read(p []byte) (n int, err error) {
	// 如果缓冲区为空，读取新的消息
	if len(r.buf) == 0 {
		r.readStart = time.Now()

		// 读取消息头
		headerBytes := make([]byte, 16)
		if _, err := io.ReadFull(r.conn, headerBytes); err != nil {
			return 0, err
		}

		// 解析消息头
		header, err := protocol.ParseHeader(headerBytes)
		if err != nil {
			return 0, err
		}
		r.currentHeader = header

		// 读取消息体
		bodySize := header.MessageLength - 16
		body := make([]byte, bodySize)
		if _, err := io.ReadFull(r.conn, body); err != nil {
			return 0, err
		}

		// 记录读取时间
		readDuration := time.Since(r.readStart)
		if r.proxy != nil {
			if r.isResponse {
				// For responses, update the trace using ResponseTo (matches the original RequestID)
				r.proxy.updateTrace(header, r.direction, readDuration)
			} else {
				// For requests, we add to trace in onMessage but still update time
				r.proxy.updateTrace(header, r.direction, readDuration)
			}
		}

		// 调用回调函数
		if r.onMessage != nil {
			r.onMessage(header, body)
		}

		// 将完整的消息放入缓冲区
		r.buf = append(headerBytes, body...)
	}

	// 从缓冲区复制数据到输出
	n = copy(p, r.buf)
	r.buf = r.buf[n:]

	// If we've emptied the buffer and this is a response, mark the trace as complete
	if len(r.buf) == 0 && r.isResponse && r.proxy != nil && r.currentHeader != nil {
		r.proxy.completeTrace(r.currentHeader.ResponseTo)
		r.currentHeader = nil
	}

	return n, nil
}

// timingWriter wraps an io.Writer to measure write times for tracing
type timingWriter struct {
	io.Writer
	direction     string
	proxy         *Proxy
	currentHeader *protocol.MsgHeader
}

func (w *timingWriter) Write(p []byte) (n int, err error) {
	// If the buffer is at least 16 bytes, try to parse the header
	if len(p) >= 16 {
		header, err := protocol.ParseHeader(p[:16])
		if err == nil {
			w.currentHeader = header
		}
	}

	writeStart := time.Now()
	n, err = w.Writer.Write(p)
	writeDuration := time.Since(writeStart)

	// Record the write time
	if w.proxy != nil && w.currentHeader != nil {
		w.proxy.updateTrace(w.currentHeader, w.direction, writeDuration)
	}

	return n, err
}
