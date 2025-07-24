# DynamoDB Metrics Collection Tool updated 17-07-2025

> **💡 Simple Alternative**: If you prefer a simple script without any programming language runtime, check out the [Bash Script Version](./dynamo_metrics_collection.sh) or [macOS Script Version](./dynamo_metrics_collection_mac.sh) that only requires AWS CLI and standard Unix tools.

A comprehensive tool for collecting DynamoDB metrics from CloudWatch. This repository provides both **Go** and **Bash** implementations.

## 📋 Quick Links

- **🖥️ Go Version** (This README) - Cross-platform, compiled binary
- **🐚 [Bash Script](./dynamo_metrics_collection.sh)** - Linux/Unix systems, no compilation needed
- **🍎 [macOS Script](./dynamo_metrics_collection_mac.sh)** - macOS optimized version

## Features

- **Dual Collection Periods**: Collects metrics for both 3-hour (20-minute intervals) and 7-day (24-hour intervals) periods
- **Multi-Region Support**: Process tables across multiple AWS regions
- **Flexible Authentication**: Support for AWS profiles, default credentials, and EC2 instance profiles
- **Comprehensive Metrics**: Collects sample counts and P99 latency for all DynamoDB operations
- **Rate Limiting**: Built-in AWS API call tracking with configurable wait thresholds
- **Log Consolidation**: Automatically consolidates raw metric files into organized summaries
- **Detailed Logging**: Comprehensive logging with timestamps and log levels

## Prerequisites

- Go 1.21 or later
- AWS CLI configured (for default region detection)
- Appropriate AWS permissions for DynamoDB and CloudWatch

## Installation

1. Clone or download the source code
2. Install dependencies:
   ```bash
   go mod tidy
   ```
3. Build the binary:
   ```bash
   go build -o get_dynamodb_metrics
   ```

## Usage

### Basic Usage

```bash
# Process all tables in the default region
./get_dynamodb_metrics

# Process specific tables
./get_dynamodb_metrics -t table1,table2,table3

# Process tables in specific regions
./get_dynamodb_metrics -r us-east-1,us-west-2

# Use a specific AWS profile
./get_dynamodb_metrics -p my-profile

# Use EC2 instance profile
./get_dynamodb_metrics -I
```

### Command Line Options

- `-t, --tables`: Comma-separated list of specific tables to process
- `-p, --profile`: AWS profile to use for authentication
- `-r, --regions`: Comma-separated list of regions to process
- `-I, --instance-profile`: Use EC2 Instance Profile for authentication
- `-w, --wait-threshold`: Number of AWS calls before waiting (default: 1000)

### Examples

```bash
# Process only 'mytable' in current region
./get_dynamodb_metrics -t mytable

# Process multiple tables in multiple regions
./get_dynamodb_metrics -t table1,table2,table3 -r us-east-1,us-west-2

# Use specific profile and regions
./get_dynamodb_metrics -p production -r us-east-1

# Use instance profile with custom wait threshold
./get_dynamodb_metrics -I -w 500
```

## Docker Support

### Build and Run with Docker

```bash
# Build the image
docker build -t dynamodb-metrics .

# Run with Docker Compose
docker-compose run dynamodb-metrics --help

# Run with specific parameters
docker-compose run dynamodb-metrics -t table1,table2 -r us-east-1
```

### Docker Compose Examples

```bash
# Show help
docker-compose run dynamodb-metrics

# Run with AWS profile
AWS_PROFILE=production docker-compose run dynamodb-metrics -p production

# Run with environment variables
AWS_DEFAULT_REGION=us-east-1 docker-compose run dynamodb-metrics -r us-east-1
```

### Interactive Container Shell for AWS Configuration

For advanced AWS authentication scenarios (Instance Profile, AWS Roles, etc.), you can attach a shell to the container and configure AWS credentials interactively:

```bash
# Start an interactive shell in the container
docker-compose run --rm dynamodb-metrics /bin/sh

# Or build and run directly with Docker
docker build -t dynamodb-metrics .
docker run -it --rm dynamodb-metrics /bin/sh
```

#### Configuring AWS Credentials in Container

Once inside the container shell, you can configure AWS credentials:

```bash
# Configure AWS CLI interactively
aws configure

# Or configure with specific profile
aws configure --profile my-profile

# Set environment variables for AWS credentials
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# For AWS Role/Instance Profile usage
export AWS_ROLE_ARN="arn:aws:iam::123456789012:role/MyRole"
export AWS_SESSION_TOKEN="your-session-token"  # If using temporary credentials

# Test AWS credentials
aws sts get-caller-identity

# Run the metrics collection tool
./get_dynamodb_metrics -I  # Use instance profile
# or
./get_dynamodb_metrics -p my-profile  # Use configured profile
```

#### Using AWS Roles and Instance Profiles

For EC2 instances or containers with IAM roles:

```bash
# Set up for instance profile usage
export AWS_DEFAULT_REGION="us-east-1"

# The container will automatically use instance metadata if available
./get_dynamodb_metrics -I

# For cross-account role assumption
aws sts assume-role --role-arn "arn:aws:iam::123456789012:role/CrossAccountRole" --role-session-name "MetricsCollection"
# Copy the credentials from the output and set environment variables
export AWS_ACCESS_KEY_ID="ASIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
```

#### Container Environment Variables

You can also pass AWS configuration via environment variables when running the container:

```bash
# Run with environment variables
docker run -it --rm \
  -e AWS_ACCESS_KEY_ID="your-access-key" \
  -e AWS_SECRET_ACCESS_KEY="your-secret-key" \
  -e AWS_DEFAULT_REGION="us-east-1" \
  -e AWS_SESSION_TOKEN="your-session-token" \
  dynamodb-metrics

# Or with Docker Compose
AWS_ACCESS_KEY_ID="your-access-key" \
AWS_SECRET_ACCESS_KEY="your-secret-key" \
AWS_DEFAULT_REGION="us-east-1" \
docker-compose run dynamodb-metrics
```

## Output Structure

The tool creates a timestamped log directory with the following structure:

```
dynamo_metrics_logs_MMDDYYHHMMSS/
├── script_execution_YYYYMMDD_HHMMSS.log
├── table_detailed.log
└── region_name/
    └── table_name/
        ├── GetItem/
        │   ├── sample_count/
        │   │   └── GetItem_SampleCount_YYYYMMDDHHMMSStoYYYYMMDDHHMMSS.log
        │   └── p99_latency/
        │       └── p99_GetItem_YYYYMMDDHHMMSStoYYYYMMDDHHMMSS.log
        ├── Query/
        ├── Scan/
        ├── PutItem/
        ├── UpdateItem/
        ├── DeleteItem/
        ├── BatchWriteItem/
        ├── table_name_GetItem_sample_count-3hr.log
        ├── table_name_GetItem_p99_latency-3hr.log
        ├── table_name_GetItem_sample_count-7day.log
        └── table_name_GetItem_p99_latency-7day.log
```

## Metrics Collected

### Operations Supported
- **Read Operations**: GetItem, Query, Scan
- **Write Operations**: PutItem, UpdateItem, DeleteItem, BatchWriteItem

### Metric Types
- **Sample Count**: Number of successful requests
- **P99 Latency**: 99th percentile latency

### Collection Periods
- **3-Hour Collection**: 9 iterations with 20-minute intervals (1-second period)
- **7-Day Collection**: 7 iterations with 24-hour intervals (60-second period)

## AWS Permissions Required

The tool requires the following AWS permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:ListTables",
                "dynamodb:DescribeTable"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:GetMetricStatistics"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sts:GetCallerIdentity"
            ],
            "Resource": "*"
        }
    ]
}
```

## Rate Limiting

The tool implements intelligent rate limiting:

- Tracks total AWS API calls made during execution
- Uses a separate wait counter that resets after each wait period
- Configurable wait threshold (default: 1000 calls)
- Automatic waiting when threshold is reached

## Error Handling

- Comprehensive error logging with timestamps
- Graceful handling of AWS API errors
- Continues processing other tables/regions if one fails
- Detailed error messages for troubleshooting

## Logging

The tool provides detailed logging with the following levels:
- **INFO**: General execution information
- **DEBUG**: Detailed debugging information
- **ERROR**: Error conditions and failures
- **WARN**: Warning conditions

All logs are written to both console and log files for easy debugging and monitoring.

## Performance Considerations

- Sequential AWS API calls (no parallel processing to avoid overwhelming APIs)
- Efficient file I/O with proper resource management
- Minimal memory footprint
- Configurable wait thresholds for rate limiting

## Differences from Bash Version

### Advantages of Go Version
- **Better Error Handling**: More robust error handling and recovery
- **Type Safety**: Compile-time type checking
- **Performance**: Generally faster execution
- **Maintainability**: Easier to maintain and extend
- **Cross-Platform**: Single binary works on multiple platforms
- **Dependencies**: No external dependencies required at runtime

### Key Features Maintained
- Same command-line interface
- Identical output structure
- Same collection periods and metrics
- Rate limiting behavior
- Log consolidation functionality

## Troubleshooting

### Common Issues

1. **AWS Credentials Not Found**
   - Ensure AWS CLI is configured: `aws configure`
   - Check environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
   - Verify profile exists: `aws configure list-profiles`

2. **Permission Denied**
   - Verify IAM permissions for DynamoDB and CloudWatch
   - Check if using correct AWS profile
   - Ensure EC2 instance has proper IAM role (if using instance profile)

3. **No Tables Found**
   - Verify region contains DynamoDB tables
   - Check table names if using `-t` flag
   - Ensure proper permissions for `dynamodb:ListTables`

4. **Rate Limiting Issues**
   - Reduce wait threshold with `-w` flag
   - Check CloudWatch API quotas
   - Monitor AWS API call limits

### Debug Mode

For detailed debugging, the tool logs extensive information including:
- AWS API calls being made
- Counter values and wait conditions
- File operations and directory creation
- Error details and stack traces

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is provided as-is for educational and operational purposes.
