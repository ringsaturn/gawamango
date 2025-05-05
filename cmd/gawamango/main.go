package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ringsaturn/gawamango/internal/proxy"
)

func main() {
	// 解析命令行参数
	listenAddr := flag.String("listen", "localhost:27018", "Address to listen on")
	targetAddr := flag.String("target", "localhost:27017", "Target MongoDB address")
	silent := flag.Bool("silent", false, "Silent mode")
	flag.Parse()

	// 创建代理实例
	p, err := proxy.NewProxy(*listenAddr, *targetAddr, proxy.WithSilent(*silent))
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	// 启动代理
	go func() {
		if err := p.Start(); err != nil {
			log.Fatalf("Failed to start proxy: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 优雅关闭
	if err := p.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
