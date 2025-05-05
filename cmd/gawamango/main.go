package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ringsaturn/gawamango/internal/proxy"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// 解析命令行参数
	listenAddr := flag.String("listen", "localhost:27018", "Address to listen on")
	targetAddr := flag.String("target", "localhost:27017", "Target MongoDB address")
	silent := flag.Bool("silent", false, "Silent mode")
	production := flag.Bool("production", false, "Production mode")

	flag.Parse()

	opts := []proxy.ProxyOption{
		proxy.WithSilent(*silent),
	}

	{
		var (
			logger    *zap.Logger
			loggerErr error
		)
		if *production {
			logger, loggerErr = zap.NewProduction()
		} else {
			encoderConfig := zap.NewDevelopmentEncoderConfig()
			encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
			config := zap.NewDevelopmentConfig()
			config.EncoderConfig = encoderConfig
			logger, loggerErr = config.Build()
		}
		if loggerErr != nil {
			log.Fatalf("Failed to create logger: %v", loggerErr)
		}
		opts = append(opts, proxy.WithLogger(logger))
	}

	// 创建代理实例
	p, err := proxy.NewProxy(*listenAddr, *targetAddr, opts...)
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
