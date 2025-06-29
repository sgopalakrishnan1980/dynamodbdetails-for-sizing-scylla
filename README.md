# DynamoDB Metrics Collection Script

A comprehensive script for collecting DynamoDB table metrics and performance data to assist with capacity planning and performance analysis.

## Overview

This script collects detailed metrics from DynamoDB tables including:
- Table details and configuration information
- CloudWatch metrics for read/write operations
- Sample counts and P99 latency measurements
- Multi-region support
- Comprehensive logging and error handling

## Prerequisites

- AWS CLI installed and configured
- `jq` for JSON processing
- Bash shell environment
- AWS credentials with appropriate permissions for:
  - DynamoDB: `dynamodb:ListTables`, `dynamodb:DescribeTable`
  - CloudWatch: `cloudwatch:GetMetricStatistics`

## Installation

1. Clone the repository
2. Make the scripts executable:
```bash
chmod +x dynamo_metrics_collection.sh
chmod +x dynamo_metrics_collection_mac.sh
```

## Usage

### Linux/Unix Version
```bash
./dynamo_metrics_collection.sh [options]
```

### macOS Version
```bash
./dynamo_metrics_collection_mac.sh [options]
```

### Options

- `-t <table_name>`  Optional: Specific table to process. If not provided, all tables will be processed.
- `-p <aws_profile>` Optional: AWS profile to use
- `-r <regions>`     Optional: Comma-separated list of regions to process. If not provided, uses current region.

### Examples

```bash
# Process all tables in current region
./dynamo_metrics_collection.sh

# Process only 'mytable' in current region
./dynamo_metrics_collection.sh -t mytable

# Process all tables in us-east-1
./dynamo_metrics_collection.sh -r us-east-1

# Process 'mytable' in specified regions with custom profile
./dynamo_metrics_collection.sh -t mytable -r us-east-1,us-west-2 -p my-aws-profile
```

## Script Flow

```ascii
+------------------+
|   Script Start   |
+------------------+
         |
         v
+------------------+
| Parse Arguments  |
+------------------+
         |
         v
+------------------+
| AWS Configuration|
| & Credentials    |
+------------------+
         |
         v
+------------------+
| Region Detection |
+------------------+
         |
         v
+------------------+
| Get Table List   |
| Across Regions   |
+------------------+
         |
         v
+------------------+
| Process Tables   |
| Sequentially     |
+------------------+
         |
         v
+------------------+
| For Each Table:  |
| 1. Get Table Info|
| 2. Collect       |
|    Sample Counts |
| 3. Collect P99   |
|    Latency       |
| 4. Process 8     |
|    Time Windows  |
+------------------+
         |
         v
+------------------+
| Generate Output  |
| Files            |
+------------------+
         |
         v
+------------------+
|  Script End      |
+------------------+
```

## Output Structure

The script creates a `dynamo_metrics_logs` directory containing:

```
dynamo_metrics_logs/
├── script_execution_YYYYMMDD_HHMMSS.log
├── table_detailed.log
└── {table_name}/
    ├── GetItem/
    │   ├── sample_count/
    │   │   └── GetItem_SampleCount_*.log
    │   └── p99_latency/
    │       └── p99_GetItem_*.log
    ├── Query/
    │   ├── sample_count/
    │   │   └── Query_SampleCount_*.log
    │   └── p99_latency/
    │       └── p99_Query_*.log
    ├── Scan/
    │   ├── sample_count/
    │   │   └── Scan_SampleCount_*.log
    │   └── p99_latency/
    │       └── p99_Scan_*.log
    ├── PutItem/
    │   ├── sample_count/
    │   │   └── PutItem_SampleCount_*.log
    │   └── p99_latency/
    │       └── *.log
    ├── UpdateItem/
    │   ├── sample_count/
    │   │   └── UpdateItem_SampleCount_*.log
    │   └── p99_latency/
    │       └── *.log
    └── DeleteItem/
        ├── sample_count/
        │   └── DeleteItem_SampleCount_*.log
        └── p99_latency/
            └── *.log
```

## Key Features

### Multi-Region Support
- Automatically detects default region
- Supports processing tables across multiple regions
- Region-specific AWS CLI calls

### Comprehensive Logging
- Detailed execution logs with timestamps
- Function call tracking with arguments
- Error handling and debugging information
- Separate log files for each execution

### Metrics Collection
- **Sample Counts**: Number of successful requests for each operation
- **P99 Latency**: 99th percentile latency measurements
- **Time Windows**: 8 iterations covering 3 hours of data
- **Operations**: GetItem, Query, Scan, PutItem, UpdateItem, DeleteItem

### Performance Optimizations
- Background processing for parallel AWS calls
- Rate limiting with configurable thresholds
- Efficient file organization by table and operation
- Sequential processing to avoid API limits

## Key Functions

### `get_sample_counts`
- Collects SampleCount statistics for all operations
- Processes read and write operations separately
- Creates organized directory structure
- Background processing for parallel execution

### `get_p99_latency`
- Collects P99 latency measurements
- Uses extended statistics for percentile data
- Separate processing for read/write operations
- Timestamp-based file naming

### `check_aws_credentials`
- Validates AWS credentials and permissions
- Supports AWS profiles
- EC2 instance profile detection
- Comprehensive error reporting

### `get_default_region`
- Multiple region detection methods
- Environment variable support
- AWS CLI configuration fallback
- Instance metadata support

## Performance Considerations

1. **Time Windows**: 8 iterations of 20-minute periods
2. **Period**: 1-second granularity for detailed metrics
3. **Parallel Processing**: Background execution for AWS calls
4. **Rate Limiting**: Configurable call thresholds with pauses
5. **File Organization**: Structured output for easy analysis

## Error Handling

- AWS credential verification
- Table existence and access checks
- CloudWatch API error handling
- Region validation
- Comprehensive logging of all operations
- Graceful failure handling with detailed error messages

## Platform Differences

### Linux/Unix Version (`dynamo_metrics_collection.sh`)
- Uses `date -d` for time calculations
- Standard Linux date command syntax
- Full multi-region support

### macOS Version (`dynamo_metrics_collection_mac.sh`)
- Uses `date -v` for time calculations
- macOS-compatible date command syntax
- Same functionality as Linux version

## Recent Updates

1. **Multi-region Support**: Process tables across multiple AWS regions
2. **Enhanced Logging**: Comprehensive logging with timestamps and levels
3. **AWS Profile Support**: Use specific AWS profiles for authentication
4. **Improved Error Handling**: Better error messages and recovery
5. **Background Processing**: Parallel AWS calls for improved performance
6. **Structured Output**: Organized file structure for easy analysis
7. **Rate Limiting**: Configurable thresholds to respect AWS API limits
8. **Table Filtering**: Process specific tables or all tables
9. **Comprehensive Metrics**: Sample counts and P99 latency for all operations

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
