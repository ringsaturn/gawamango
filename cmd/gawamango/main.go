package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ringsaturn/gawamango/internal/proxy"
	"github.com/ringsaturn/gawamango/internal/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	listenAddr := flag.String("listen", ":27018", "Address to listen on")
	targetAddr := flag.String("target", "localhost:27017", "Target MongoDB address")
	silent := flag.Bool("silent", false, "Silent mode")
	production := flag.Bool("production", false, "Production mode")
	flag.Parse()

	opts := []proxy.ProxyOption{
		proxy.WithSilent(*silent),
	}

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

	// 创建代理
	p, err := proxy.NewProxy(*listenAddr, *targetAddr, opts...)
	if err != nil {
		log.Fatalf("failed to create proxy: %v", err)
	}

	// 启动代理
	if err := p.Start(); err != nil {
		log.Fatalf("failed to start proxy: %v", err)
	}

	// 记录启动信息，包括当前使用的导出器类型
	exporterType := telemetry.GetExporterTypeFromEnv()
	logger.Info("Proxy started",
		zap.String("listen", *listenAddr),
		zap.String("target", *targetAddr),
		zap.String("telemetry_exporter", string(exporterType)),
	)

	// 等待信号
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	// 关闭代理
	logger.Info("Shutting down...")
	if err := p.Stop(); err != nil {
		logger.Error("Failed to stop proxy", zap.Error(err))
		os.Exit(1)
	}
}
