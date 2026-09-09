# Quboid

A high-performance, multi-tier data management system built in Rust that provides intelligent storage routing, priority-based task scheduling, and seamless integration between cache, database, and object storage layers.

## Description

Quboid is designed as a DataBricks-inspired object storage solution that intelligently routes data to the appropriate storage backend based on size, format, and caching requirements. It provides a unified API for managing data across three storage tiers:

- **Cache Layer**: Fast, temporary storage using Redis-compatible cache (Rustis)
- **Database Layer**: Structured storage with MySQL support and format-based data import
- **Data Lake**: Large-scale object storage with automatic chunking and deduplication

## Why Quboid?

Modern data-intensive applications require flexible storage strategies that can adapt to varying data characteristics. Quboid addresses several critical challenges:

- **Intelligent Storage Routing**: Automatically selects the optimal storage backend based on data size, format, and access patterns
- **Priority-Based Processing**: Implements a priority queue system for task execution, ensuring critical operations are handled first
- **Multi-Tier Architecture**: Seamlessly integrates cache, database, and object storage for optimal performance
- **Format-Based Data Import**: Parse and import structured data using custom format specifications
- **Batch Processing**: Efficiently handle large volumes of data with configurable batch sizes
- **RESTful API**: Built-in HTTP server with Axum for easy integration into web applications

## Features

- **Priority Queue System**: Four priority levels (Low, Default, Medium, High) for task scheduling
- **Multi-Layer Caching**: L1, L2, L3 cache layers with automatic routing
- **Object Storage**: Automatic data chunking with configurable blob sizes
- **Data Integrity**: Hash-based verification and dirty state detection
- **Format Parsing**: Custom format specification for structured data import
- **Web API**: RESTful endpoints for task submission and finalization
- **Async/Await**: Fully asynchronous architecture using Tokio
- **Metadata Tracking**: Comprehensive metadata including UUIDs, checksums, and maintainer info

## Installation

Add Quboid to your `Cargo.toml`:

```toml
[dependencies]
Quboid = "0.1.0"
```

Or install directly:

```bash
cargo add Quboid
```

## Usage

### Basic Setup

```rust
use Quboid::Qube;
use std::path::PathBuf;

// Create a new Qube instance
let mut qube = Qube::new(
    None,                    // Optional metadata
    100,                     // Batch size
    "mysql://user:pass@localhost/db",  // Database URL
    Some(PathBuf::from("./data")),     // Object storage path
    1024 * 1024              // Blob size (1MB)
);
```

### Submitting Tasks

```rust
use Quboid::model::misc::misc::Priority;

// Submit a single task with default priority
qube.submit(
    "sample data".to_string(),
    Some(Priority::High),
    None,  // No custom rules
    None   // No format specification
);

// Submit with custom rules and format
let mut rules = std::collections::HashMap::new();
rules.insert("FMT".to_string(), "{name:TEXT},{age:INTEGER}".to_string());

qube.submit(
    "John,25\nJane,30".to_string(),
    Some(Priority::Medium),
    Some(rules),
    Some("{name:TEXT},{age:INTEGER}".to_string())
);
```

### Batch Submission

```rust
let tasks = vec![
    ("data1".to_string(), Some(Priority::High), None, None),
    ("data2".to_string(), Some(Priority::Low), None, None),
    ("data3".to_string(), Some(Priority::Default), None, None),
];

qube.submit_all(tasks);
```

### Finalizing Tasks

```rust
// Process all pending tasks
qube.finalise().await;

// Process tasks in batches
qube.batch_finalise().await;
```

### Running as a Web Server

```rust
use Quboid::Qube;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let qube = Qube::new(None, 100, "mysql://localhost/db", None, 1024);
    
    // Start the HTTP server
    qube.Host("0.0.0.0".to_string(), 8080, Some("/api".to_string())).await?;
    
    Ok(())
}
```

## API Endpoints

When running as a web server, Quboid provides the following REST endpoints:

- `POST /api/submit` - Submit a single task
- `POST /api/submit/all` - Submit multiple tasks
- `GET /api/finalise` - Finalize all pending tasks
- `GET /api/finalise/batch` - Finalize tasks in batches

### Submit Task Example

```bash
curl -X POST http://localhost:8080/api/submit \
  -H "Content-Type: application/json" \
  -d '{
    "data": "sample data",
    "prio": "High",
    "rule": {"FMT": "{name:TEXT},{value:INTEGER}"}
  }'
```

## Format Specification

Quboid supports custom format specifications for structured data import:

```rust
// Format: {column_name:type}[separator]
// Example: "{name:TEXT},{age:INTEGER},{email:TEXT}"

let format = "{name:TEXT},{age:INTEGER},{email:TEXT}";
let data = "John,25,john@example.com\nJane,30,jane@example.com";

qube.submit(data.to_string(), None, None, Some(format.to_string()));
```

Supported types:
- `TEXT` - String data
- `INTEGER` - Integer values
- Custom SQL types can be specified

## Priority Levels

Tasks can be assigned four priority levels (ordered from lowest to highest):

- `Priority::Low` - Background tasks
- `Priority::Default` - Standard processing
- `Priority::Medium` - Important tasks
- `Priority::High` - Critical operations

## Cache Operations

Quboid supports various cache operations through the rule system:

```rust
let mut rules = std::collections::HashMap::new();
rules.insert("insert".to_string(), "true".to_string());

let data = "KEY:my_key\nVALUE:my_value\nTTL:3600";
qube.submit(data.to_string(), None, Some(rules), None);
```

Supported cache commands:
- `insert` - Add a key-value pair
- `delete` - Remove a key
- `get` - Retrieve a value
- `update` - Update a value
- `contains` - Check key existence

## Data Lake Operations

For large data storage, Quboid provides object storage operations:

```rust
let mut rules = std::collections::HashMap::new();
rules.insert("ADD".to_string(), "true".to_string());

let large_data = "very large data content...".repeat(10000);
qube.submit(large_data, None, Some(rules), None);
```

Supported lake commands:
- `ADD` / `INSERT` - Add data to the lake
- `RETRIEVE` / `GET` - Retrieve stored data
- `DELETE` / `REMOVE` - Delete data
- `IS_DIRTY` - Check data integrity
- `DATA_TO_ID` - Convert data to ID
- `ID_TO_DATA` - Convert ID to data

## Command Line Interface

Quboid includes a CLI for debugging:

```bash
# Run with debug mode
cargo run -- -d

# Show help
cargo run -- -h
```

## Configuration

### Environment Variables

Create a `.env` file in your project root:

```
DATABASE_URL=mysql://user:password@localhost/quboid
CACHE_HOST=0.0.0.0
CACHE_PORT=8080
```

### Docker Compose

Use the provided `docker-compose.yml` for containerized deployment:

```bash
docker-compose up -d
```

## Examples

### Example 1: Simple Data Storage

```rust
use Quboid::Qube;

#[tokio::main]
async fn main() {
    let mut qube = Qube::new(
        None,
        50,
        "mysql://localhost/test",
        Some(std::path::PathBuf::from("./storage")),
        1024
    );
    
    qube.submit("Hello, World!".to_string(), None, None, None);
    qube.finalise().await;
}
```

### Example 2: CSV Import with Format

```rust
use Quboid::Qube;
use Quboid::model::misc::misc::Priority;

#[tokio::main]
async fn main() {
    let mut qube = Qube::new(None, 100, "mysql://localhost/db", None, 1024);
    
    let csv_data = "Alice,30,Engineer\nBob,25,Designer\nCharlie,35,Manager";
    let format = "{name:TEXT},{age:INTEGER},{role:TEXT}";
    
    qube.submit(csv_data.to_string(), Some(Priority::High), None, Some(format.to_string()));
    qube.finalise().await;
}
```

### Example 3: Web Server with API

```rust
use Quboid::Qube;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let qube = Qube::new(
        None,
        100,
        "mysql://localhost/quboid",
        Some(std::path::PathBuf::from("./object_store")),
        1024 * 1024  // 1MB blobs
    );
    
    println!("Starting server on http://0.0.0.0:8080");
    qube.Host("0.0.0.0".to_string(), 8080, None).await?;
    
    Ok(())
}
```

## License

This project is licensed under GPL-3.0-only - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Architecture

Quboid's architecture consists of several key components:

- **Qube**: Main orchestrator managing task queues and storage routing
- **PrioScheduler**: Priority-based task scheduler with batch processing
- **DataLake**: Object storage with automatic chunking and deduplication
- **DataBase**: MySQL integration with format-based import
- **CacheConnector**: Redis-compatible cache interface
- **Router**: Axum-based HTTP API server

## Performance Considerations

- **Batch Size**: Configure based on your workload (default: 100)
- **Blob Size**: Set according to your data characteristics (default: 1MB)
- **Cache Layers**: Use L1 for hot data, L2/L3 for warm data
- **Priority Assignment**: Reserve High priority for critical operations

## Troubleshooting

### Database Connection Issues
Ensure your MySQL server is running and the connection string is correct in your `.env` file.

### Cache Connection Errors
Verify that your Rustis (or Redis-compatible) server is accessible at the configured host and port.

### Storage Permission Errors
Check that the application has write permissions for the object storage directory.

## Support

For issues, questions, or contributions, please visit the project repository.
