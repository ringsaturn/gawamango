package proxy

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestProxy(t *testing.T) {
	// 创建测试用的MongoDB服务器模拟器
	serverListener, err := net.Listen("tcp", "localhost:7777")
	if err != nil {
		t.Fatalf("Failed to create test server: %v", err)
	}
	defer serverListener.Close()

	// 启动测试服务器
	go func() {
		conn, err := serverListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// 简单的echo服务器
		buf := make([]byte, 1024)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			conn.Write(buf[:n])
		}
	}()

	// 创建代理实例
	proxyAddr := "localhost:6666"
	p, err := NewProxy(proxyAddr, serverListener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}
	defer p.Stop()

	// 连接到代理
	conn, err := net.Dial("tcp", p.listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to connect to proxy: %v", err)
	}
	defer conn.Close()

	// 测试数据
	testData := []byte("test message")
	if _, err := conn.Write(testData); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}

	// 读取响应
	buf := make([]byte, len(testData))
	if _, err := conn.Read(buf); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// 验证响应
	if string(buf) != string(testData) {
		t.Errorf("Expected response %q, got %q", string(testData), string(buf))
	}
}

func TestProxyShutdown(t *testing.T) {
	p, err := NewProxy("localhost:6666", "localhost:7777")
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		t.Fatalf("Failed to start proxy: %v", err)
	}

	// 创建取消上下文
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
