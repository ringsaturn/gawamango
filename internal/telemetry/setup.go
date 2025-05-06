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

// SetupOTelSDK bootstraps the OpenTelemetry pipeline.
// It relies on environment variables for configuration.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(ctx context.Context, serviceName, serviceVersion string) (shutdown func(context.Context) error, err error) {
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

	// Set up resource with service information
	res, err := newResource(serviceName, serviceVersion)
	if err != nil {
		handleErr(err)
		return
	}

	// Set up propagator
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider
	tracerProvider, err := newTracerProvider(ctx, res)
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

// newTracerProvider creates a new TracerProvider with exporters configured
// from environment variables.
// Standard environment variables:
// - OTEL_TRACES_EXPORTER: The exporter to use (e.g., "otlp", "stdout")
// - OTEL_EXPORTER_OTLP_ENDPOINT: The OTLP endpoint URL
// - OTEL_EXPORTER_OTLP_PROTOCOL: The OTLP protocol (e.g., "http/protobuf", "grpc")
func newTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	// Use the exporter specified by OTEL_TRACES_EXPORTER
	exporterType := os.Getenv("OTEL_TRACES_EXPORTER")
	if exporterType == "" {
		exporterType = "otlp" // Default to OTLP exporter
	}

	var exporter sdktrace.SpanExporter
	var err error

	switch exporterType {
	case "stdout":
		// Set up stdout exporter to print traces to console
		exporter, err = stdouttrace.New(
			stdouttrace.WithPrettyPrint(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout exporter: %w", err)
		}
		fmt.Println("Using stdout exporter: trace data will be output to console")
	case "otlp":
		// Use environment variables for OTLP configuration (OTEL_EXPORTER_OTLP_*)
		clientOptions := []otlptracehttp.Option{}

		// Check if we're connecting to localhost - if so, default to HTTP unless explicitly configured
		endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:4318"
			clientOptions = append(clientOptions, otlptracehttp.WithEndpoint("localhost:4318"))
			clientOptions = append(clientOptions, otlptracehttp.WithInsecure())
		} else if endpoint == "localhost:4318" || endpoint == "http://localhost:4318" || endpoint == "https://localhost:4318" {
			// For localhost, default to HTTP unless HTTPS is explicitly specified
			if endpoint != "https://localhost:4318" {
				clientOptions = append(clientOptions, otlptracehttp.WithInsecure())
			}
		}

		// Allow explicit configuration of insecure mode via environment variable
		if insecure := os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"); insecure == "true" {
			clientOptions = append(clientOptions, otlptracehttp.WithInsecure())
		}

		client := otlptracehttp.NewClient(clientOptions...)
		exporter, err = otlptrace.New(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
		}
		fmt.Println("Using OTLP exporter: trace data will be sent to the configured endpoint")
	default:
		return nil, fmt.Errorf("unsupported exporter type: %s", exporterType)
	}

	_ = exporter
	// Create the TracerProvider with the configured exporter
	// The SDK will read other settings from environment variables
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
	)

	return tracerProvider, nil
}
