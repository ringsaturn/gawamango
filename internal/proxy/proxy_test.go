package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// simpleEchoProxy is a simple proxy that just forwards data without MongoDB protocol parsing
type simpleEchoProxy struct {
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func newSimpleEchoProxy(addr string) (*simpleEchoProxy, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &simpleEchoProxy{
		ctx:    ctx,
		cancel: cancel,
	}
	return p, nil
}

func (p *simpleEchoProxy) Start() error {
	var err error
	p.listener, err = net.Listen("tcp", "localhost:6666")
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	go p.acceptLoop()
	return nil
}

func (p *simpleEchoProxy) Stop() error {
	p.cancel()
	if p.listener != nil {
		p.listener.Close()
	}
	p.wg.Wait()
	return nil
}

func (p *simpleEchoProxy) acceptLoop() {
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

func (p *simpleEchoProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	// Connect to the target server
	serverConn, err := net.Dial("tcp", "localhost:7777")
	if err != nil {
		return
	}
	defer serverConn.Close()

	// Set short timeouts for both connections
	clientConn.SetDeadline(time.Now().Add(100 * time.Millisecond))
	serverConn.SetDeadline(time.Now().Add(100 * time.Millisecond))

	// Simple bidirectional copy
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(serverConn, clientConn)
	}()

	go func() {
		defer wg.Done()
		io.Copy(clientConn, serverConn)
	}()

	wg.Wait()
}

func TestProxy(t *testing.T) {
	// 创建测试用的MongoDB服务器模拟器
	serverListener, err := net.Listen("tcp", "localhost:7777")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer serverListener.Close()

	// 创建上下文用于控制测试服务器的生命周期
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动测试服务器
	go func() {
		conn, err := serverListener.Accept()
		if err != nil {
			t.Logf("Test server accept error: %v", err)
			return
		}
		defer conn.Close()

		// 设置读写超时
		conn.SetDeadline(time.Now().Add(100 * time.Millisecond))

		// 简单的echo服务器
		buf := make([]byte, 1024)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n, err := conn.Read(buf)
				if err != nil {
					if err != io.EOF {
						t.Logf("Test server read error: %v", err)
					}
					return
				}
				if _, err := conn.Write(buf[:n]); err != nil {
					t.Logf("Test server write error: %v", err)
					return
				}
			}
		}
	}()

	// 创建代理实例
	p, err := newSimpleEchoProxy("localhost:6666")
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer p.Stop()

	// 连接到代理
	conn, err := net.Dial("tcp", "localhost:6666")
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer conn.Close()

	// 设置读写超时
	conn.SetDeadline(time.Now().Add(100 * time.Millisecond))

	// 测试数据
	testData := []byte("test message")
	if _, err := conn.Write(testData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// 读取响应
	buf := make([]byte, len(testData))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// 验证响应
	if n != len(testData) {
		t.Errorf("Expected %d bytes, got %d bytes", len(testData), n)
	}
	if string(buf[:n]) != string(testData) {
		t.Errorf("Expected response %q, got %q", string(testData), string(buf[:n]))
	}
}

func TestProxyShutdown(t *testing.T) {
	p, err := newSimpleEchoProxy("localhost:6666")
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}

	// 创建取消上下文
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// 在单独的goroutine中停止代理
	done := make(chan error)
	go func() {
		done <- p.Stop()
	}()

	// 等待停止完成或超时
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Stop returned error: %v", err)
		}
	case <-ctx.Done():
		t.Error("Stop timed out")
	}
}
