# Data Nexus

Data Nexus is a distributed system for receiving, processing, and exporting metrics, written in Go. The project provides a gRPC API for metric ingestion, uses Redis Streams as the message broker, and exposes metrics in a Prometheus-compatible format through an HTTP endpoint.

## Features

- **gRPC Ingestion:** Accepts single or batched metrics via gRPC.
- **Redis Broker:** Utilizes Redis Streams for message queuing and distributed processing.
- **Prometheus Exporter:** Exposes metrics through an HTTP endpoint in Prometheus format.
- **In-Memory Storage:** Temporary storage for metrics before exporting.
- **Flexible Configuration:** Managed through functional options.

## Architecture

The project consists of several main components:

- **Server (package `datanexus`):** The main application that initializes and manages all internal services.
- **Broker (package `broker`):** Handles publishing and consuming messages using Redis Streams.
- **gRPC Server (package `grpcserver`):** Provides an API for metric ingestion.
- **Metrics Server (package `metrics`):** Exposes metrics in Prometheus format.
- **Background Workers (package `workers`):** Processes incoming metrics, acknowledges them, redistributes messages, and updates the server's state.
- **Storage (package `storage`):** Storage for metrics.
- **Configuration (package `config`):** Manages application settings through functional options.
- **Logging (package `logging`):** Provides configurable logging.
- **Types (package `types`):** Defines data structures like metrics and server state.

## Requirements

- **Redis:** A running Redis instance for message brokering.
- **gRPC and Protocol Buffers:** Required to generate Go code from `.proto` files if modifying the API.

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/haze518/data-nexus.git
   cd data-nexus
   ```

2. **Install dependencies:**
    ```bash
    go mod download
    ```

3. **Build the project:**
    ```bash
    go build -o datanexus cmd/datanexus/main.go
    ```

## Configuration

The project uses the config package to manage settings via functional options. Default values are set in config.NewConfig(). You can override them using option functions such as:

* WithGrpcAddr(addr string)
* WithHttpAddr(addr string)
* WithRedisConfig(config RedisConfig)
* WithLoggingConfig(config LoggingConfig)
* WithWorkerConfig(config WorkerConfig)
* WithMetricsConfig(config MetricsConfig)

## Example

```go
cfg := config.NewConfig(
    config.WithGrpcAddr("0.0.0.0:50051"),
    config.WithHttpAddr("0.0.0.0:8080"),
    config.WithWorkerConfig(config.WorkerConfig{
        HeartbeatInterval:     1 * time.Second,
        ConsumerInterval:      1 * time.Second,
        RedistributorInterval: 2 * time.Second,
        SinkerInterval:        1 * time.Second,
        AckerInterval:         1 * time.Second,
        BatchSize:             10,
    }),
)
```

## Running the Server

Make sure Redis is running. Then, execute the compiled binary:
```bash
./datanexus
```
The server will work as follows:

* gRPC Service: Accepts metrics at the specified gRPC address.
* HTTP Service: Exposes metrics for Prometheus at the specified HTTP address (e.g., /metrics endpoint).

## API

### gRPC Ingestion
* IngestMetric: Accepts a single metric via gRPC.
* IngestMetrics: Accepts a batch of metrics via gRPC.

Detailed API documentation can be found in the .proto files in the proto directory.

### HTTP Metrics
* /metrics: Exposes metrics in Prometheus format. On request, it drains the storage of metrics and outputs the data with metadata (HELP and TYPE).
