package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/ringsaturn/mangopi/internal/protocol"
)

// Proxy represents a MongoDB proxy instance
type Proxy struct {
	listenAddr string
	targetAddr string
	listener   net.Listener
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewProxy creates a new MongoDB proxy instance
func NewProxy(listenAddr, targetAddr string) (*Proxy, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &Proxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
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
		p.listener.Close()
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

func (p *Proxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	// 连接到目标MongoDB服务器
	serverConn, err := net.Dial("tcp", p.targetAddr)
	if err != nil {
		return
	}
	defer serverConn.Close()

	// 创建双向数据转发
	var wg sync.WaitGroup
	wg.Add(2)

	// 客户端到服务器的转发
	go func() {
		defer wg.Done()
		// 创建自定义的Reader来解析MongoDB消息
		reader := &mongoReader{
			conn: clientConn,
			onMessage: func(header *protocol.MsgHeader, body []byte) {
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
			},
		}
		_, err := io.Copy(serverConn, reader)
		if err != nil && p.ctx.Err() == nil {
			fmt.Printf("Error copying from client to server: %v\n", err)
		}
	}()

	// 服务器到客户端的转发
	go func() {
		defer wg.Done()
		_, err := io.Copy(clientConn, serverConn)
		if err != nil && p.ctx.Err() == nil {
			fmt.Printf("Error copying from server to client: %v\n", err)
		}
	}()

	// 等待上下文取消或连接关闭
	select {
	case <-p.ctx.Done():
		clientConn.Close()
		serverConn.Close()
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		return done
	}():
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
