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

	"github.com/ringsaturn/gawamango/internal/protocol"
	"github.com/ringsaturn/gawamango/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	listenAddr   string
	targetAddr   string
	listener     net.Listener
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	silent       bool
	logger       *zap.Logger
	tracer       trace.Tracer
	otelShutdown func(context.Context) error
}

// NewProxy creates a new MongoDB proxy instance
func NewProxy(listenAddr, targetAddr string, opts ...ProxyOption) (*Proxy, error) {
	ctx, cancel := context.WithCancel(context.Background())

	p := &Proxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		ctx:        ctx,
		cancel:     cancel,
	}

	for _, opt := range opts {
		opt(p)
	}

	// 初始化 OpenTelemetry
	exporterType := telemetry.GetExporterTypeFromEnv()
	shutdown, err := telemetry.SetupOTelSDK(ctx, "mongodb-proxy", "0.1.0", exporterType)
	if err != nil {
		return nil, fmt.Errorf("failed to setup OpenTelemetry: %v", err)
	}
	p.otelShutdown = shutdown

	// 获取 tracer
	p.tracer = otel.Tracer("github.com/ringsaturn/gawamango/internal/proxy")

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

	// 关闭 OpenTelemetry
	if p.otelShutdown != nil {
		if err := p.otelShutdown(context.Background()); err != nil {
			return fmt.Errorf("error shutting down OpenTelemetry: %v", err)
		}
	}

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

	ctx, span := p.tracer.Start(p.ctx, "handle_connection")
	defer span.End()

	// Connect to the target MongoDB server.
	serverConn, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		if p.logger != nil {
			p.logger.Error("error connecting to target server", zap.Error(err))
		}

		span.SetStatus(codes.Error, "failed to connect to MongoDB server")
		span.RecordError(err)

		if err := clientConn.Close(); err != nil {
			p.logger.Error("error closing client connection", zap.Error(err))
		}

		return
	}

	// 创建连接级别的context
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	// client → server
	go func() {
		defer wg.Done()

		clientToServerCtx, clientToServerSpan := p.tracer.Start(connCtx, "client_to_server")
		defer clientToServerSpan.End()

		// 使用 otel instrumented 的reader和writer
		_, err := p.copyWithTracing(clientToServerCtx, serverConn, clientConn, "client_to_server")

		// Half-close: signal FIN for this direction only
		if tcp, ok := serverConn.(*net.TCPConn); ok {
			if err := tcp.CloseWrite(); err != nil && !isBrokenPipe(err) {
				p.logger.Error("error closing write on server connection", zap.Error(err))
			}
		} else {
			if err := serverConn.Close(); err != nil {
				p.logger.Error("error closing server connection", zap.Error(err))
			}
		}

		// Log error if not a normal termination
		if err != nil && !isBrokenPipe(err) && connCtx.Err() == nil {
			p.logger.Error("error copying from client to server", zap.Error(err))
			clientToServerSpan.SetStatus(codes.Error, "error copying from client to server")
			clientToServerSpan.RecordError(err)
		}
	}()

	// server → client
	go func() {
		defer wg.Done()

		serverToClientCtx, serverToClientSpan := p.tracer.Start(connCtx, "server_to_client")
		defer serverToClientSpan.End()

		// 使用 otel instrumented 的reader和writer
		_, err := p.copyWithTracing(serverToClientCtx, clientConn, serverConn, "server_to_client")

		// Half-close: signal FIN for this direction only
		if tcp, ok := clientConn.(*net.TCPConn); ok {
			if err := tcp.CloseWrite(); err != nil && !isBrokenPipe(err) {
				p.logger.Error("error closing write on client connection", zap.Error(err))
			}
		} else {
			if err := clientConn.Close(); err != nil {
				p.logger.Error("error closing client connection", zap.Error(err))
			}
		}

		// Log error if not a normal termination
		if err != nil && !isBrokenPipe(err) && connCtx.Err() == nil {
			p.logger.Error("error copying from server to client", zap.Error(err))
			serverToClientSpan.SetStatus(codes.Error, "error copying from server to client")
			serverToClientSpan.RecordError(err)
		}
	}()

	// Wait for both directions to finish
	wg.Wait()

	// Close connections
	if err := clientConn.Close(); err != nil {
		p.logger.Error("error closing client connection", zap.Error(err))
	}
	if err := serverConn.Close(); err != nil {
		p.logger.Error("error closing server connection", zap.Error(err))
	}
}

// copyWithTracing 使用 OpenTelemetry 跟踪 MongoDB 消息从 src 到 dst 的传输
func (p *Proxy) copyWithTracing(ctx context.Context, dst io.Writer, src io.Reader, direction string) (written int64, err error) {
	// 创建 buffer 用于读取消息头
	buf := make([]byte, 16)

	for {
		// 检查 context 是否已取消
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		// 1. 读取消息头 (16 字节)
		n, err := io.ReadFull(src, buf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return written, nil // 正常结束
			}
			return written, err
		}
		written += int64(n)

		// 2. 解析消息头
		header, err := protocol.ParseHeader(buf)
		if err != nil {
			return written, err
		}

		// 创建 span 用于记录单个 MongoDB 消息的处理
		_, msgSpan := p.tracer.Start(ctx, "mongodb_message")
		msgSpan.SetAttributes(
			attribute.Int("message.length", int(header.MessageLength)),
			attribute.Int("request.id", int(header.RequestID)),
			attribute.Int("response.to", int(header.ResponseTo)),
			attribute.Int("op.code", int(header.OpCode)),
			attribute.String("direction", direction),
		)

		// 3. 写入消息头到目标
		if _, err := dst.Write(buf); err != nil {
			msgSpan.RecordError(err)
			msgSpan.SetStatus(codes.Error, "failed to write message header")
			msgSpan.End()
			return written, err
		}

		// 4. 计算消息体长度并读取
		bodySize := header.MessageLength - 16

		// 读取和写入消息体
		if bodySize > 0 {
			// 分配一个足够大的 buffer 来读取消息体
			bodyBuf := make([]byte, bodySize)
			n, err := io.ReadFull(src, bodyBuf)
			if err != nil {
				msgSpan.RecordError(err)
				msgSpan.SetStatus(codes.Error, "failed to read message body")
				msgSpan.End()
				return written + int64(n), err
			}
			written += int64(n)

			// 如果这是请求消息，尝试解析命令
			if header.ResponseTo == 0 && !p.silent {
				cmd, err := protocol.ParseCommand(header.OpCode, bodyBuf)
				if err == nil && cmd != nil {
					msgSpan.SetAttributes(
						attribute.String("mongodb.command", cmd.CommandName),
						attribute.String("mongodb.database", cmd.Database),
					)

					if p.logger != nil {
						p.logger.Info("MongoDB Command",
							zap.String("direction", direction),
							zap.Int32("requestID", header.RequestID),
							zap.String("command", cmd.CommandName),
							zap.String("database", cmd.Database),
						)
					}
				}
			}

			// 写入消息体到目标
			if _, err := dst.Write(bodyBuf); err != nil {
				msgSpan.RecordError(err)
				msgSpan.SetStatus(codes.Error, "failed to write message body")
				msgSpan.End()
				return written, err
			}
		}

		// 结束当前消息的 span
		msgSpan.End()
	}
}
