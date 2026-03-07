package telemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
)

// loggingErrorHandler routes internal OTel errors to stderr for easier debugging.
type loggingErrorHandler struct{}

func (loggingErrorHandler) Handle(err error) {
	fmt.Fprintf(os.Stderr, "otel internal error: %v\n", err)
}

func InitOTel(ctx context.Context, serviceName string, otlpEndpoint string) (func(context.Context) error, error) {
	// Enable extra debug when requested via envs
	// - AIRFLOW_EXPORTER_LOG_LEVEL=debug or OTEL_LOG_LEVEL=debug will enable gRPC verbose logs
	debug := strings.EqualFold(os.Getenv("AIRFLOW_EXPORTER_LOG_LEVEL"), "debug") || strings.EqualFold(os.Getenv("OTEL_LOG_LEVEL"), "debug")
	if debug {
		// Set gRPC verbosity via env and log to stdout/stderr
		_ = os.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "info")
		_ = os.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", "2")
		grpclog.SetLoggerV2(grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stderr))
		// Route OTel internal errors
		otel.SetErrorHandler(loggingErrorHandler{})
		// Announce endpoint received
		fmt.Fprintf(os.Stderr, "otel: init with endpoint=%q\n", otlpEndpoint)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// gRPC Metrics Exporter only
	exporterCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	grpcOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		otlpmetricgrpc.WithDialOption(grpc.WithBlock()),
	}
	if otlpEndpoint != "" {
		grpcOpts = append(grpcOpts, otlpmetricgrpc.WithEndpoint(otlpEndpoint))
		grpcOpts = append(grpcOpts, otlpmetricgrpc.WithInsecure())
		fmt.Fprintf(os.Stderr, "otel:grpc: using insecure to %q\n", otlpEndpoint)
	}
	metricExporter, err := otlpmetricgrpc.New(exporterCtx, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}
	fmt.Fprintf(os.Stderr, "otel:grpc: exporter created successfully\n")

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(30*time.Second))),
	)
	otel.SetMeterProvider(meterProvider)

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return meterProvider.Shutdown, nil
}

func CheckOTel(ctx context.Context, timeout time.Duration) error {
	// Try to force flush metrics to verify exporter connectivity
	if mp, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider); ok {
		flushCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return mp.ForceFlush(flushCtx)
	}
	return fmt.Errorf("meter provider is not SDK; cannot force flush")
}
