package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ExporterType 定义导出器类型
type ExporterType string

const (
	// ExporterTypeOTLP 表示使用 OTLP 导出器
	ExporterTypeOTLP ExporterType = "otlp"
	// ExporterTypeStdout 表示使用 stdout 导出器
	ExporterTypeStdout ExporterType = "stdout"

	// 环境变量名
	envExporterType = "OTEL_EXPORTER_TYPE"
)

// GetExporterTypeFromEnv 从环境变量获取导出器类型
func GetExporterTypeFromEnv() ExporterType {
	exporterType := os.Getenv(envExporterType)
	if exporterType == "" {
		return ExporterTypeOTLP // 默认使用 OTLP
	}

	switch ExporterType(exporterType) {
	case ExporterTypeOTLP:
		return ExporterTypeOTLP
	case ExporterTypeStdout:
		return ExporterTypeStdout
	default:
		fmt.Printf("未知的导出器类型 %q，使用默认的 OTLP 导出器\n", exporterType)
		return ExporterTypeOTLP
	}
}

// SetupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(ctx context.Context, serviceName, serviceVersion string, exporterType ExporterType) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up resource.
	res, err := newResource(serviceName, serviceVersion)
	if err != nil {
		handleErr(err)
		return
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTracerProvider(ctx, res, exporterType)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	return
}

func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTracerProvider(ctx context.Context, res *resource.Resource, exporterType ExporterType) (*sdktrace.TracerProvider, error) {
	var exporter sdktrace.SpanExporter
	var err error

	switch exporterType {
	case ExporterTypeStdout:
		// 设置 stdout 导出器，将数据打印到控制台
		exporter, err = stdouttrace.New(
			stdouttrace.WithPrettyPrint(), // 使用格式化的 JSON 输出，便于阅读
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
		fmt.Println("使用 stdout 导出器，跟踪数据将直接输出到控制台")
	case ExporterTypeOTLP:
		// 设置 OTLP 导出器
		client := otlptracehttp.NewClient()
		exporter, err = otlptrace.New(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
		fmt.Println("使用 OTLP 导出器，跟踪数据将发送到配置的端点")
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", exporterType)
	}

	// 创建 TracerProvider
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			// 默认是 5s，这里设为 1s 用于演示目的
			sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
	)

	return tracerProvider, nil
}
