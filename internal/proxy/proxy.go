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
	"github.com/ringsaturn/gawamango/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
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
	// requestID -> span (kept until the matching response arrives)
	// inflight sync.Map // removed as per instructions
}

// reqState keeps the root span and the time the request finished writing to MongoDB
type reqState struct {
	span        trace.Span
	sentAt      time.Time
	commandName string
	database    string
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
	shutdown, err := telemetry.SetupOTelSDK(ctx, "gawamango", Version, exporterType)
	if err != nil {
		return nil, fmt.Errorf("failed to setup OpenTelemetry: %v", err)
	}
	p.otelShutdown = shutdown

	// 获取 tracer，确保使用有意义的名称
	p.tracer = otel.Tracer("gawamango")

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

	ctx := p.ctx

	// Connect to the target MongoDB server.
	serverConn, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		if p.logger != nil {
			p.logger.Error("error connecting to target server", zap.Error(err))
		}

		if err := clientConn.Close(); err != nil {
			p.logger.Error("error closing client connection", zap.Error(err))
		}

		return
	}

	// 创建连接级别的context
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	inflight := &sync.Map{}

	var wg sync.WaitGroup
	wg.Add(2)

	// client → server
	go func() {
		defer wg.Done()

		clientToServerCtx := connCtx

		// 使用 otel instrumented 的reader和writer
		_, err := p.copyWithTracing(clientToServerCtx, serverConn, clientConn, "client_to_server", inflight)

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
		}
	}()

	// server → client
	go func() {
		defer wg.Done()

		serverToClientCtx := connCtx

		// 使用 otel instrumented 的reader和writer
		_, err := p.copyWithTracing(serverToClientCtx, clientConn, serverConn, "server_to_client", inflight)

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
		}
	}()

	// Wait for both directions to finish
	wg.Wait()

	// End any spans that never got a response (e.g., due to timeout or retry)
	inflight.Range(func(key, value any) bool {
		if span, ok := value.(trace.Span); ok {
			span.SetStatus(codes.Error, "connection closed before response")
			span.End()
		}
		inflight.Delete(key)
		return true
	})

	// Close connections
	if err := clientConn.Close(); err != nil {
		p.logger.Error("error closing client connection", zap.Error(err))
	}
	if err := serverConn.Close(); err != nil {
		p.logger.Error("error closing server connection", zap.Error(err))
	}
}

// copyWithTracing 使用 OpenTelemetry 跟踪 MongoDB 消息从 src 到 dst 的传输
func (p *Proxy) copyWithTracing(ctx context.Context, dst io.Writer, src io.Reader, direction string, inflight *sync.Map) (written int64, err error) {
	// 创建 buffer 用于读取消息头
	buf := make([]byte, 16)

	var cmdName, dbName string

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

		// --- tracing logic implementing: write_to_server / mongo_processing / write_to_client ---
		var (
			rootSpan  trace.Span // the cmd_xxx span
			writeSpan trace.Span // write_to_* span
			procSpan  trace.Span // mongo_processing span
			stateAny  any
			ok        bool
		)

		// Case 1: client → server, this is a request (ResponseTo == 0)
		if direction == "client_to_server" && header.ResponseTo == 0 {
			// root span
			_, rootSpan = p.tracer.Start(ctx, fmt.Sprintf("mongo.cmd_%d", header.RequestID))

			// write_to_server span (child)
			_, writeSpan = p.tracer.Start(trace.ContextWithSpan(ctx, rootSpan), "write_to_server")
		}

		// Case 2: server → client, this is a response
		if direction == "server_to_client" && header.ResponseTo != 0 {
			// pick the stored state
			if stateAny, ok = inflight.LoadAndDelete(header.ResponseTo); ok {
				st := stateAny.(*reqState)
				rootSpan = st.span

				// propagate cached command/database attributes if not already set
				if st.commandName != "" {
					rootSpan.SetAttributes(
						attribute.String("mongodb.command", st.commandName),
						attribute.String("mongodb.database", st.database),
					)
				}

				// mongo_processing span: from sentAt until now
				_, procSpan = p.tracer.Start(trace.ContextWithSpan(ctx, rootSpan),
					"mongo_processing",
					trace.WithTimestamp(st.sentAt))
				procSpan.End() // end immediately with "now"
				// write_to_client span (child)
				_, writeSpan = p.tracer.Start(trace.ContextWithSpan(ctx, rootSpan), "write_to_client")
			}
		}

		// common attributes
		if rootSpan != nil {
			attrs := []attribute.KeyValue{
				attribute.Int("message.length", int(header.MessageLength)),
				attribute.Int("request.id", int(header.RequestID)),
				attribute.Int("response.to", int(header.ResponseTo)),
				attribute.Int("op.code", int(header.OpCode)),
				attribute.String("op.name", protocol.OpcodeToName(header.OpCode)),
				attribute.String("direction", direction),
			}
			rootSpan.SetAttributes(attrs...)
		}

		// 3. 写入消息头到目标
		if _, err := dst.Write(buf); err != nil {
			if rootSpan != nil {
				rootSpan.RecordError(err)
				rootSpan.SetStatus(codes.Error, "failed to write message header")
				rootSpan.End()
			}
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
				if rootSpan != nil {
					rootSpan.RecordError(err)
					rootSpan.SetStatus(codes.Error, "failed to read message body")
					rootSpan.End()
				}
				return written + int64(n), err
			}
			written += int64(n)

			// 如果这是请求消息，尝试解析命令
			if header.ResponseTo == 0 && !p.silent {
				cmd, err := protocol.ParseCommand(header.OpCode, bodyBuf)
				if err == nil && cmd != nil {
					if rootSpan != nil {
						rootSpan.SetAttributes(
							attribute.String("mongodb.command", cmd.CommandName),
							attribute.String("mongodb.database", cmd.Database),
						)
					}
					// save for response side
					cmdName, dbName = cmd.CommandName, cmd.Database

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
				if rootSpan != nil {
					rootSpan.RecordError(err)
					rootSpan.SetStatus(codes.Error, "failed to write message body")
					rootSpan.End()
				}
				return written, err
			}

			// finish write_to_* span if it exists
			if writeSpan != nil {
				writeSpan.End()
			}

			// store reqState after finishing write_to_server
			if direction == "client_to_server" && header.ResponseTo == 0 {
				inflight.Store(header.RequestID, &reqState{
					span:        rootSpan,
					sentAt:      time.Now(),
					commandName: cmdName,
					database:    dbName,
				})
			}

			// end root span when response direction completed
			if direction == "server_to_client" && rootSpan != nil {
				rootSpan.End()
			}
		}
	}
}
