# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A multi-language toolkit for collecting DynamoDB CloudWatch metrics and processing them into Excel format for capacity planning and ScyllaDB sizing. Provides Go (compiled binary), Bash scripts (no runtime), and Python (Excel processing) implementations.

**Purpose**: Enable ScyllaDB migration sizing by analyzing DynamoDB production workload patterns (operations per second, P99 latencies, table sizes).

## Architecture

### Three-Component System

**1. Metrics Collection** (Choose one)
- **`main.go`** + **`get_dynamodb_metrics`** (Go binary) - Recommended for cross-platform use
- **`dynamo_metrics_collection.sh`** (Bash) - Linux/Unix, no compilation needed
- **`dynamo_metrics_collection_mac.sh`** (Bash) - macOS-optimized version

**2. Excel Processing**
- **`process_dynamodb_logs.py`** (Python) - Converts raw logs to formatted Excel spreadsheets

**3. Visualization** (Optional)
- **`postprocess/postprocess.py`** (Python + Dash) - Interactive web dashboard for metrics

### Data Flow

```
AWS DynamoDB → CloudWatch API → Metrics Collection (Go/Bash)
    ↓
Raw Logs (CSV/JSON) in timestamped directories
    ↓
Python Excel Processor → Formatted .xlsx files per region
    ↓
(Optional) Dash UI → Interactive visualizations
```

## Technology Stack

**Go Binary** (`main.go`):
- Go 1.21+
- AWS SDK v2 (CloudWatch, DynamoDB, STS)
- Cobra CLI framework
- Supports: AWS profiles, instance profiles, multi-region

**Bash Scripts**:
- Pure Bash + AWS CLI
- Standard Unix tools (sed, awk, date)
- No external dependencies

**Python Processor** (`process_dynamodb_logs.py`):
- Python 3.x
- openpyxl (Excel generation)
- Supports CSV and JSON log formats

**Visualization** (`postprocess/`):
- Dash + Plotly for interactive charts
- JSON data storage

## Key Components

### 1. Go Binary (`main.go`)

**Key Functions**:
- `main()`: CLI entry point with Cobra commands
- `getMetrics()`: CloudWatch API interaction for sample counts and P99 latency
- `processTable()`: Per-table metric collection orchestration
- `consolidateLogs()`: Aggregates raw metric files into consolidated summaries
- Rate limiting with configurable wait thresholds

**Metrics Collected** (per DynamoDB operation):
- **Sample Count**: Number of successful requests
- **P99 Latency**: 99th percentile response time
- Operations: GetItem, Query, Scan, PutItem, UpdateItem, DeleteItem, BatchWriteItem

**Collection Periods**:
- **3-Hour Window**: 9 iterations × 20-minute intervals (1-second period granularity)
- **7-Day Window**: 7 iterations × 24-hour intervals (60-second period granularity)

### 2. Bash Script (`dynamo_metrics_collection.sh`)

**Purpose**: Alternative to Go binary for environments where compiling Go is not feasible

**Key Features**:
- Direct AWS CLI usage for DynamoDB and CloudWatch
- Same output structure as Go version
- Shell-based rate limiting
- Identical collection periods and metrics

### 3. Python Processor (`process_dynamodb_logs.py`)

**Key Functions**:
- `parse_csv_format()`: Extract datapoints from CSV logs
- `parse_json_format()`: Extract datapoints from JSON CloudWatch responses
- `parse_table_detailed_log()`: Extract table metadata from ASCII table format
- `process_region()`: Generate Excel file per region
- `create_excel_workbook()`: Build formatted Excel with multiple worksheets

**Excel Output Structure**:
- Table Summary worksheet (metadata + max values)
- Per-table worksheets (sample counts + P99 latencies)
- Professional formatting with headers and auto-adjusted columns

### 4. Visualization Dashboard (`postprocess/postprocess.py`)

**CLI Commands**:
- `load --folder <path>`: Parse logs into JSON data files
- `UI`: Launch Dash web interface

**Dashboard Tabs**:
- Sample Count: Per-table operation counts
- P99 Latency: Per-operation latency visualization
- Metadata: Tables, regions, operations context

## Common Development Commands

### Build and Install (Go Binary)

```bash
# Install dependencies
go mod tidy

# Build for current platform
go build -o get_dynamodb_metrics

# Or use Makefile
make build              # Current platform
make build-all          # Linux, macOS, Windows
make deps               # Download dependencies
```

### Run Metrics Collection

```bash
# Go binary - all tables in default region
./get_dynamodb_metrics

# Specific tables and regions
./get_dynamodb_metrics -t table1,table2 -r us-east-1,us-west-2

# With AWS profile
./get_dynamodb_metrics -p production -r us-east-1

# With EC2 instance profile
./get_dynamodb_metrics -I

# Custom rate limit threshold
./get_dynamodb_metrics -w 500

# Bash version (Linux/Unix)
./dynamo_metrics_collection.sh

# Bash version (macOS)
./dynamo_metrics_collection_mac.sh
```

### Process Logs to Excel

```bash
# Install Python dependencies
pip install -r requirements.txt

# Process CSV format logs
python process_dynamodb_logs.py \
  -t /path/to/dynamo_metrics_logs_080525142223 \
  -d /tmp/excel_output \
  -c customer_name \
  -f csv

# Process JSON format logs
python process_dynamodb_logs.py \
  -t /path/to/json/logs \
  -d /tmp/excel_output \
  -c customer_name \
  -f json
```

### Visualize Data

```bash
# Install visualization dependencies
pip install -r postprocess/requirements.txt

# Load metrics into JSON
python postprocess/postprocess.py load --folder path/to/dynamo_metrics_logs/

# Launch dashboard
python postprocess/postprocess.py UI
# Opens browser at http://localhost:8050
```

### Docker Usage

```bash
# Build image
docker build -t dynamodb-metrics .

# Run with Docker Compose
docker-compose run dynamodb-metrics -r us-east-1

# Run with environment variables
AWS_PROFILE=production docker-compose run dynamodb-metrics -p production

# Interactive shell for AWS configuration
docker-compose run --rm dynamodb-metrics /bin/sh
# Then inside container:
aws configure
./get_dynamodb_metrics -r us-east-1
```

## Configuration and Authentication

### AWS Authentication Methods

1. **AWS Profile** (Recommended for local development):
```bash
./get_dynamodb_metrics -p my-profile
```

2. **Default Credentials**:
```bash
aws configure
./get_dynamodb_metrics
```

3. **Environment Variables**:
```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_DEFAULT_REGION="us-east-1"
./get_dynamodb_metrics
```

4. **EC2 Instance Profile**:
```bash
./get_dynamodb_metrics -I
```

### Required AWS Permissions

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
      "Action": ["cloudwatch:GetMetricStatistics"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["sts:GetCallerIdentity"],
      "Resource": "*"
    }
  ]
}
```

## Output Structure

### Metrics Collection Output

```
dynamo_metrics_logs_MMDDYYHHMMSS/
├── script_execution_YYYYMMDD_HHMMSS.log
├── table_detailed.log                          # Table metadata
└── region_name/
    └── table_name/
        ├── GetItem/
        │   ├── sample_count/
        │   │   └── GetItem_SampleCount_*.log
        │   └── p99_latency/
        │       └── p99_GetItem_*.log
        ├── Query/
        ├── Scan/
        ├── PutItem/
        ├── UpdateItem/
        ├── DeleteItem/
        ├── BatchWriteItem/
        ├── table_name_GetItem_sample_count-3hr.log     # Consolidated
        ├── table_name_GetItem_p99_latency-3hr.log
        ├── table_name_GetItem_sample_count-7day.log
        └── table_name_GetItem_p99_latency-7day.log
```

### Excel Processing Output

```
output_directory/
├── customer_region1_dynamodb_metrics_YYYYMMDD_HHMMSS.xlsx
├── customer_region2_dynamodb_metrics_YYYYMMDD_HHMMSS.xlsx
└── customer_YYYYMMDD_HHMMSS.log
```

### Visualization Output

```
postprocess/data/
├── sample_count_data.json
└── sample_p99_data.json
```

## Data Formats

### CSV Format (Raw Logs)
```
DATAPOINTS 2935.0 2025-08-05T15:29:00+00:00 Milliseconds
```

### JSON Format (CloudWatch API)
```json
{
  "Datapoints": [
    {
      "Timestamp": "2025-08-05T15:29:00Z",
      "SampleCount": 2935.0
    }
  ]
}
```

### ASCII Table Format (table_detailed.log)
```
||  TableSizeBytes           |  1156168014599                        ||
||  ItemCount                |  1670604447                           ||
||  BillingMode              |  PAY_PER_REQUEST                      ||
```

## Development Patterns

### Adding New DynamoDB Operation

1. **Go Binary** (`main.go`):
```go
operations := []string{"GetItem", "Query", "Scan", "PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "NewOperation"}
```

2. **Bash Script**:
```bash
for operation in GetItem Query Scan PutItem UpdateItem DeleteItem BatchWriteItem NewOperation; do
    # Collection logic
done
```

3. **Python Processor**: Auto-discovers operations from log structure (no change needed)

### Customizing Collection Windows

**Go Binary** (`main.go`):
```go
// 3-hour collection
iterations3Hr := 9
interval3Hr := 20 * time.Minute
period3Hr := 1 // seconds

// 7-day collection
iterations7Day := 7
interval7Day := 24 * time.Hour
period7Day := 60 // seconds
```

### Rate Limiting Tuning

```bash
# Default: wait after 1000 AWS API calls
./get_dynamodb_metrics -w 1000

# More aggressive: wait after 500 calls
./get_dynamodb_metrics -w 500

# Less frequent: wait after 2000 calls
./get_dynamodb_metrics -w 2000
```

## Troubleshooting

### Common Issues

**AWS Credentials Not Found**:
```bash
aws configure
# or
export AWS_PROFILE=my-profile
# or
./get_dynamodb_metrics -I  # Use instance profile
```

**No Tables Found**:
```bash
# Verify region has tables
aws dynamodb list-tables --region us-east-1

# Check specific tables
./get_dynamodb_metrics -t table1,table2 -r us-east-1
```

**Excel Processing Errors**:
```bash
# Check log directory structure
ls -R /path/to/dynamo_metrics_logs_*/

# Verify Python dependencies
pip install -r requirements.txt

# Check file permissions
chmod -R 755 /path/to/logs
```

**Rate Limiting Issues**:
```bash
# Reduce API call rate
./get_dynamodb_metrics -w 500

# Check CloudWatch quotas
aws service-quotas get-service-quota \
  --service-code monitoring \
  --quota-code L-5E141212
```

### Debugging

**Go Binary**:
- Logs to `script_execution_YYYYMMDD_HHMMSS.log`
- Contains AWS API call details, counter values, errors

**Python Processor**:
- Logs to `customer_YYYYMMDD_HHMMSS.log`
- Records missing data, processing steps, errors

**Bash Script**:
- Console output with timestamps
- Error messages to stderr

## Performance Considerations

- **Sequential Processing**: No parallel AWS API calls to avoid overwhelming APIs
- **Efficient I/O**: Proper resource management, minimal memory footprint
- **Rate Limiting**: Configurable thresholds prevent API throttling
- **Large Tables**: May take hours for 7-day collection on high-throughput tables

## Cross-Platform Compatibility

**Go Binary**:
- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

**Bash Scripts**:
- Linux/Unix: Use `dynamo_metrics_collection.sh`
- macOS: Use `dynamo_metrics_collection_mac.sh` (handles BSD date differences)

**Python**:
- Platform-independent (requires Python 3.x)

## Related Files

- **README.md**: Comprehensive usage documentation
- **PROCESSING_FLOWCHART.md**: Visual guide to processing logic
- **PYTHON_README.md**: Detailed Python processor documentation
- **SHELL_SCRIPTS_README.md**: Bash script documentation
- **Makefile**: Build automation for Go binary
- **Dockerfile** + **docker-compose.yml**: Container deployment
- **requirements.txt**: Python dependencies for Excel processing
- **postprocess/requirements.txt**: Python dependencies for visualization

## Use Cases

1. **ScyllaDB Migration Sizing**: Analyze DynamoDB workload to size ScyllaDB clusters
2. **Capacity Planning**: Understand table growth and operation patterns
3. **Performance Analysis**: Identify high-latency operations and hot tables
4. **Cost Optimization**: Analyze per-table usage for billing optimization
5. **Multi-Region Comparison**: Compare table performance across AWS regions
