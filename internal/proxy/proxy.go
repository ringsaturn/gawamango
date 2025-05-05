package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/ringsaturn/gawamango/internal/protocol"
)

type ProxyOption func(*Proxy)

func WithSilent(silent bool) ProxyOption {
	return func(p *Proxy) {
		p.silent = silent
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

func loggingMsg(header *protocol.MsgHeader, body []byte) {
	// 解析命令
	cmd, err := protocol.ParseCommand(header.OpCode, body)
	if err != nil {
		fmt.Printf("Error parsing command: %v\n", err)
		return
	}

	// 打印命令信息
	cmdJSON, _ := json.MarshalIndent(cmd, "", "  ")
	fmt.Printf("MongoDB Command:\n")
	fmt.Printf("  Database: %s\n", cmd.Database)
	fmt.Printf("  Command: %s\n", cmd.CommandName)
	fmt.Printf("  Arguments: %s\n", string(cmdJSON))
	fmt.Println("----------------------------------------")
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
			if se.Err == syscall.EPIPE || se.Err == syscall.ECONNRESET {
				return true
			}
		}
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset by peer")
}

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()

	// Connect to the target MongoDB server.
	serverConn, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		if err := clientConn.Close(); err != nil {
			fmt.Printf("Error closing client connection: %v\n", err)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	onMessageOps := []func(header *protocol.MsgHeader, body []byte){}
	if !p.silent {
		onMessageOps = append(onMessageOps, loggingMsg)
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
			if err := tcp.CloseWrite(); err != nil {
				fmt.Printf("Error closing write on server connection: %v\n", err)
			}
		} else {
			if err := serverConn.Close(); err != nil {
				fmt.Printf("Error closing server connection: %v\n", err)
			}
		}

		if err != nil && !isBrokenPipe(err) && p.ctx.Err() == nil {
			fmt.Printf("Error copying from client to server: %v\n", err)
		}
	}()

	// server ➜ client
	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, serverConn)

		// Half‑close: signal FIN for this direction only.
		if tcp, ok := clientConn.(*net.TCPConn); ok {
			if err := tcp.CloseWrite(); err != nil {
				fmt.Printf("Error closing write on client connection: %v\n", err)
			}
		} else {
			if err := clientConn.Close(); err != nil {
				fmt.Printf("Error closing client connection: %v\n", err)
			}
		}

		if err != nil && !isBrokenPipe(err) && p.ctx.Err() == nil {
			fmt.Printf("Error copying from server to client: %v\n", err)
		}
	}()

	// Wait for both directions to finish.
	wg.Wait()
	if err := clientConn.Close(); err != nil {
		fmt.Printf("Error closing client connection: %v\n", err)
	}
	if err := serverConn.Close(); err != nil {
		fmt.Printf("Error closing server connection: %v\n", err)
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
