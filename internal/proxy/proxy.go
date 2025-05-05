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
	listenAddr string
	targetAddr string
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	silent     bool
	logger     *zap.Logger
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

	var wg sync.WaitGroup
	wg.Add(2)

	onMessageOps := []func(header *protocol.MsgHeader, body []byte){}
	if !p.silent {
		onMessageOps = append(onMessageOps, p.setupLoggingMsg())
	}

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
		}
		_, err := io.Copy(serverConn, reader)

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

		if err != nil && !isBrokenPipe(err) && p.ctx.Err() == nil {
			p.logger.Error("error copying from client to server", zap.Error(err))
		}
	}()

	// server ➜ client
	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, serverConn)

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

		if err != nil && !isBrokenPipe(err) && p.ctx.Err() == nil {
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
	conn      net.Conn
	onMessage func(header *protocol.MsgHeader, body []byte)
	buf       []byte
}

func (r *mongoReader) Read(p []byte) (n int, err error) {
	// 如果缓冲区为空，读取新的消息
	if len(r.buf) == 0 {
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

		// 读取消息体
		bodySize := header.MessageLength - 16
		body := make([]byte, bodySize)
		if _, err := io.ReadFull(r.conn, body); err != nil {
			return 0, err
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
	return n, nil
}
