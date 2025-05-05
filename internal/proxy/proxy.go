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

// contextKey 是用于在 context 中存储值的键类型
type contextKey string

const (
	// 用于存储请求开始时间
	startTimeKey contextKey = "start_time"
	// 用于存储 MongoDB 请求 ID
	requestIDKey contextKey = "request_id"
	// 用于存储 MongoDB 命令
	commandKey contextKey = "command"
	// 用于存储数据库名称
	databaseKey contextKey = "database"
	// 用于存储客户端到代理的时间
	clientToProxyTimeKey contextKey = "client_to_proxy_time"
	// 用于存储代理到服务器的时间
	proxyToServerTimeKey contextKey = "proxy_to_server_time"
	// 用于存储服务器到代理的时间
	serverToProxyTimeKey contextKey = "server_to_proxy_time"
	// 用于存储代理到客户端的时间
	proxyToClientTimeKey contextKey = "proxy_to_client_time"
)

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
	// 请求ID到context的映射，用于关联请求和响应
	contexts   map[int32]context.Context
	contextsMu sync.RWMutex // 保护contexts的互斥锁
}

// NewProxy creates a new MongoDB proxy instance
func NewProxy(listenAddr, targetAddr string, opts ...ProxyOption) (*Proxy, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Proxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		ctx:        ctx,
		cancel:     cancel,
		contexts:   make(map[int32]context.Context),
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

func (p *Proxy) setupLoggingMsg() func(ctx context.Context, header *protocol.MsgHeader, body []byte) context.Context {
	if p.logger == nil {
		return nil
	}

	return func(ctx context.Context, header *protocol.MsgHeader, body []byte) context.Context {
		cmd, err := protocol.ParseCommand(header.OpCode, body)
		if err != nil {
			p.logger.Error("error parsing command", zap.Error(err))
			return ctx
		}

		p.logger.Info(
			"MongoDB Command",
			zap.String("database", cmd.Database),
			zap.String("command", cmd.CommandName),
			zap.Any("arguments", cmd.Arguments),
		)

		// 将命令信息添加到context
		ctx = context.WithValue(ctx, commandKey, cmd.CommandName)
		ctx = context.WithValue(ctx, databaseKey, cmd.Database)

		return ctx
	}
}

// storeContext 保存请求的context以便在响应中查找
func (p *Proxy) storeContext(requestID int32, ctx context.Context) {
	p.contextsMu.Lock()
	defer p.contextsMu.Unlock()
	p.contexts[requestID] = ctx
}

// getContext 根据responseID获取对应请求的context
func (p *Proxy) getContext(responseID int32) (context.Context, bool) {
	p.contextsMu.RLock()
	defer p.contextsMu.RUnlock()
	ctx, exists := p.contexts[responseID]
	return ctx, exists
}

// removeContext 删除已完成请求的context
func (p *Proxy) removeContext(requestID int32) {
	p.contextsMu.Lock()
	defer p.contextsMu.Unlock()
	delete(p.contexts, requestID)
}

// createRequestContext 为新请求创建context并记录开始时间
func (p *Proxy) createRequestContext(header *protocol.MsgHeader) context.Context {
	// 用连接级别的context创建请求context
	ctx := context.WithValue(p.ctx, startTimeKey, time.Now())
	ctx = context.WithValue(ctx, requestIDKey, header.RequestID)

	// 保存此context以便在响应中找到它
	p.storeContext(header.RequestID, ctx)

	return ctx
}

// updateContextTiming 更新context中的计时信息
func (p *Proxy) updateContextTiming(ctx context.Context, key contextKey, duration time.Duration) context.Context {
	return context.WithValue(ctx, key, duration)
}

// logCompletedRequest 记录完成的请求信息
func (p *Proxy) logCompletedRequest(ctx context.Context) {
	if p.logger == nil || ctx == nil {
		return
	}

	startTime, ok := ctx.Value(startTimeKey).(time.Time)
	if !ok {
		return
	}

	requestID, _ := ctx.Value(requestIDKey).(int32)
	command, _ := ctx.Value(commandKey).(string)
	database, _ := ctx.Value(databaseKey).(string)

	clientToProxyTime, _ := ctx.Value(clientToProxyTimeKey).(time.Duration)
	proxyToServerTime, _ := ctx.Value(proxyToServerTimeKey).(time.Duration)
	serverToProxyTime, _ := ctx.Value(serverToProxyTimeKey).(time.Duration)
	proxyToClientTime, _ := ctx.Value(proxyToClientTimeKey).(time.Duration)

	// 计算服务器处理时间和总时间
	totalTime := time.Since(startTime)
	serverProcessTime := totalTime - clientToProxyTime - proxyToServerTime - serverToProxyTime - proxyToClientTime

	// 记录完整的请求信息
	p.logger.Debug("Request completed",
		zap.Int32("requestID", requestID),
		zap.String("command", command),
		zap.String("database", database),
		zap.Duration("clientToProxy", clientToProxyTime),
		zap.Duration("proxyToServer", proxyToServerTime),
		zap.Duration("serverProcess", serverProcessTime),
		zap.Duration("serverToProxy", serverToProxyTime),
		zap.Duration("proxyToClient", proxyToClientTime),
		zap.Duration("totalTime", totalTime),
	)

	// 完成后删除context
	p.removeContext(requestID)
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

	// 创建连接级别的context
	connCtx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	onMessageOps := []func(ctx context.Context, header *protocol.MsgHeader, body []byte) context.Context{}
	if !p.silent {
		onMessageOps = append(onMessageOps, p.setupLoggingMsg())
	}

	onMessageFunc := func(ctx context.Context, header *protocol.MsgHeader, body []byte) context.Context {
		for _, op := range onMessageOps {
			ctx = op(ctx, header, body)
		}
		return ctx
	}

	// client ➜ server
	go func() {
		defer wg.Done()
		reader := &mongoReader{
			conn:      clientConn,
			onMessage: onMessageFunc,
			direction: "client_to_proxy",
			proxy:     p,
			timingKey: clientToProxyTimeKey,
			ctx:       connCtx,
			createCtx: true, // 这是请求的起点，需要创建新的context
		}

		// 使用中间writer来跟踪timing
		writer := &timingWriter{
			Writer:    serverConn,
			direction: "proxy_to_server",
			proxy:     p,
			timingKey: proxyToServerTimeKey,
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
			onMessage:  nil, // 响应不需要onMessage
			direction:  "server_to_proxy",
			proxy:      p,
			timingKey:  serverToProxyTimeKey,
			ctx:        connCtx,
			createCtx:  false, // 这是响应，需要查找已有的context
			isResponse: true,
		}

		writer := &timingWriter{
			Writer:    clientConn,
			direction: "proxy_to_client",
			proxy:     p,
			timingKey: proxyToClientTimeKey,
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
	onMessage     func(ctx context.Context, header *protocol.MsgHeader, body []byte) context.Context
	buf           []byte
	currentHeader *protocol.MsgHeader
	direction     string
	proxy         *Proxy
	isResponse    bool
	readStart     time.Time
	ctx           context.Context
	msgCtx        context.Context // 当前消息的context
	createCtx     bool
	timingKey     contextKey
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
			var msgCtx context.Context

			if r.isResponse {
				// 对于响应，查找相应的请求context
				existingCtx, exists := r.proxy.getContext(header.ResponseTo)
				if exists {
					msgCtx = existingCtx
					// 更新context中的计时信息
					msgCtx = r.proxy.updateContextTiming(msgCtx, r.timingKey, readDuration)
					// 使用timingWriter负责记录proxyToClientTime并完成请求
				}
			} else if r.createCtx {
				// 对于请求，创建新的context
				msgCtx = r.proxy.createRequestContext(header)
				// 更新context中的计时信息
				msgCtx = r.proxy.updateContextTiming(msgCtx, r.timingKey, readDuration)
			}

			// 保存msgCtx以供timingWriter使用
			r.msgCtx = msgCtx

			// 调用回调函数
			if r.onMessage != nil && msgCtx != nil {
				r.msgCtx = r.onMessage(msgCtx, header, body)
			}
		}

		// 将完整的消息放入缓冲区
		r.buf = append(headerBytes, body...)
	}

	// 从缓冲区复制数据到输出
	n = copy(p, r.buf)
	r.buf = r.buf[n:]

	return n, nil
}

// timingWriter 包装io.Writer以测量写入时间
type timingWriter struct {
	io.Writer
	direction     string
	proxy         *Proxy
	currentHeader *protocol.MsgHeader
	timingKey     contextKey
}

func (w *timingWriter) Write(p []byte) (n int, err error) {
	var msgCtx context.Context
	var msgID int32

	// 如果缓冲区至少有16字节，尝试解析header
	if len(p) >= 16 {
		header, err := protocol.ParseHeader(p[:16])
		if err == nil {
			w.currentHeader = header

			// 确定是请求还是响应
			if header.ResponseTo > 0 {
				// 这是响应
				existingCtx, exists := w.proxy.getContext(header.ResponseTo)
				if exists {
					msgCtx = existingCtx
					msgID = header.ResponseTo
				}
			} else {
				// 这是请求
				existingCtx, exists := w.proxy.getContext(header.RequestID)
				if exists {
					msgCtx = existingCtx
					msgID = header.RequestID
				}
			}
		}
	}

	writeStart := time.Now()
	n, err = w.Writer.Write(p)
	writeDuration := time.Since(writeStart)

	// 记录写入时间
	if w.proxy != nil && msgCtx != nil {
		msgCtx = w.proxy.updateContextTiming(msgCtx, w.timingKey, writeDuration)

		// 如果这是发送给客户端的响应，标记请求完成
		if w.timingKey == proxyToClientTimeKey && w.currentHeader != nil && w.currentHeader.ResponseTo > 0 {
			w.proxy.logCompletedRequest(msgCtx)
		}

		// 更新context
		if msgID > 0 {
			w.proxy.storeContext(msgID, msgCtx)
		}
	}

	return n, err
}
