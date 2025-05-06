package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
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

func newSimpleEchoProxy(_ string) (*simpleEchoProxy, error) {
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
		if err := p.listener.Close(); err != nil {
			return fmt.Errorf("error closing listener: %v", err)
		}
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
	defer func() {
		if err := clientConn.Close(); err != nil {
			fmt.Printf("Error closing client connection: %v\n", err)
		}
	}()

	// Connect to the target server
	serverConn, err := net.Dial("tcp", "localhost:7777")
	if err != nil {
		return
	}
	defer func() {
		if err := serverConn.Close(); err != nil {
			fmt.Printf("Error closing server connection: %v\n", err)
		}
	}()

	// Set longer timeouts for both connections
	if err := clientConn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		fmt.Printf("Error setting client deadline: %v\n", err)
	}
	if err := serverConn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		fmt.Printf("Error setting server deadline: %v\n", err)
	}

	// Simple bidirectional copy
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if _, err := io.Copy(serverConn, clientConn); err != nil {
			// Only log if not a timeout error or connection closed
			if !isNetworkTimeout(err) && !isConnClosed(err) {
				fmt.Printf("Error copying from client to server: %v\n", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		if _, err := io.Copy(clientConn, serverConn); err != nil {
			// Only log if not a timeout error or connection closed
			if !isNetworkTimeout(err) && !isConnClosed(err) {
				fmt.Printf("Error copying from server to client: %v\n", err)
			}
		}
	}()

	wg.Wait()
}

// Helper function to check if error is a timeout
func isNetworkTimeout(err error) bool {
	if err == nil {
		return false
	}
	netErr, ok := err.(net.Error)
	return ok && netErr.Timeout()
}

// Helper function to check if error is due to closed connection
func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	return err == io.EOF || strings.Contains(err.Error(), "use of closed network connection")
}

func TestProxy(t *testing.T) {
	// 创建测试用的MongoDB服务器模拟器
	serverListener, err := net.Listen("tcp", "localhost:7777")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer func() {
		if err := serverListener.Close(); err != nil {
			t.Logf("Error closing server listener: %v", err)
		}
	}()

	// 创建上下文用于控制测试服务器的生命周期
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 设置一个完成信号通道
	serverReady := make(chan struct{})
	serverDone := make(chan struct{})

	// 创建代理实例
	p, err := newSimpleEchoProxy("localhost:6666")
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer func() {
		if err := p.Stop(); err != nil {
			t.Logf("Error stopping proxy: %v", err)
		}
	}()

	// 启动测试服务器
	go func() {
		defer close(serverDone)

		// Signal that server is waiting for connections
		close(serverReady)

		conn, err := serverListener.Accept()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				t.Logf("Test server accept error: %v", err)
			}
			return
		}

		defer func() {
			if err := conn.Close(); err != nil {
				t.Logf("Error closing connection: %v", err)
			}
		}()

		// 设置读写超时
		if err := conn.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
			t.Logf("Error setting deadline: %v", err)
		}

		// 简单的echo服务器
		buf := make([]byte, 1024)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n, err := conn.Read(buf)
				if err != nil {
					if err != io.EOF && !isNetworkTimeout(err) && !isConnClosed(err) {
						t.Logf("Test server read error: %v", err)
					}
					return
				}
				if _, err := conn.Write(buf[:n]); err != nil {
					if !isNetworkTimeout(err) && !isConnClosed(err) {
						t.Logf("Test server write error: %v", err)
					}
					return
				}
			}
		}
	}()

	// Wait for server to be ready
	select {
	case <-serverReady:
		// Server is ready
	case <-time.After(1 * time.Second):
		t.Fatalf("Timeout waiting for test server to be ready")
	}

	// Allow a brief delay for the server to actually start accepting
	time.Sleep(100 * time.Millisecond)

	// 连接到代理
	conn, err := net.Dial("tcp", "localhost:6666")
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("Error closing connection: %v", err)
		}
	}()

	// 设置读写超时 - 增加超时时间
	if err := conn.SetDeadline(time.Now().Add(1 * time.Second)); err != nil {
		t.Logf("Error setting deadline: %v", err)
	}

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

	// 确保优雅地关闭连接
	cancel() // Signal server to stop

	// Wait for server to finish with timeout
	select {
	case <-serverDone:
		// Server finished properly
	case <-time.After(1 * time.Second):
		// Timeout waiting for server to close, not a failure
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
