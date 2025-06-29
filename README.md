<<<<<<< HEAD
# DynamoDB Metrics Collection and Sizing Scripts

A comprehensive collection of scripts for analyzing DynamoDB tables, collecting performance metrics, and assisting with capacity planning and migration to ScyllaDB.

## Overview

This repository contains multiple scripts designed to help with DynamoDB analysis and sizing:

### Metrics Collection Scripts
- **`dynamo_metrics_collection.sh`** (Linux/Unix) - Collects detailed CloudWatch metrics
- **`dynamo_metrics_collection_mac.sh`** (macOS) - macOS-compatible metrics collection

### Sizing and Capacity Planning Scripts
- **`dynamodb_sizing_script.sh`** (Linux/Unix) - Comprehensive sizing analysis with multiple time windows
- **`dynamodb_sizing_script_mac.sh`** (macOS) - macOS-compatible sizing script
- **`dynamodb_sizing_script_macos.sh`** (macOS) - Alternative macOS sizing script
- **`dynamodb_sizing_basic-7day.sh`** (Linux/Unix) - Simplified 7-day analysis
=======
# DynamoDB Metrics Collection Scripts

A comprehensive collection of scripts for analyzing DynamoDB tables and collecting performance metrics to assist with capacity planning and migration to ScyllaDB.

## Overview

This repository contains scripts designed to help with DynamoDB analysis and metrics collection:

### Metrics Collection Scripts
- **`dynamo_metrics_collection.sh`** (Linux/Unix) - Collects detailed CloudWatch metrics with dual collection periods
- **`dynamo_metrics_collection_mac.sh`** (macOS) - macOS-compatible metrics collection with dual collection periods
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)

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
chmod +x *.sh
```

## Script Categories

<<<<<<< HEAD
### 1. Metrics Collection Scripts

These scripts collect detailed CloudWatch metrics for DynamoDB tables including sample counts and P99 latency measurements.
=======
### Metrics Collection Scripts

These scripts collect detailed CloudWatch metrics for DynamoDB tables including sample counts and P99 latency measurements across two collection periods:
- **3-Hour Collection**: 9 iterations with 1-second period (20-minute intervals)
- **7-Day Collection**: 7 iterations with 60-second period (24-hour intervals)
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)

#### Usage

**Linux/Unix Version:**
```bash
./dynamo_metrics_collection.sh [options]
```

**macOS Version:**
```bash
./dynamo_metrics_collection_mac.sh [options]
```

#### Options
- `-t <table_name>`  Optional: Specific table to process. If not provided, all tables will be processed.
- `-p <aws_profile>` Optional: AWS profile to use
- `-r <regions>`     Optional: Comma-separated list of regions to process. If not provided, uses current region.
- `-I`               Optional: Use EC2 Instance Profile for authentication

#### Examples
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

### 2. Sizing and Capacity Planning Scripts

These scripts provide comprehensive analysis for capacity planning and migration to ScyllaDB.

#### Usage

**Comprehensive Sizing (Linux/Unix):**
```bash
./dynamodb_sizing_script.sh [options]
```

**Basic 7-Day Analysis (Linux/Unix):**
```bash
./dynamodb_sizing_basic-7day.sh [options]
```

**macOS Versions:**
```bash
./dynamodb_sizing_script_mac.sh [options]
./dynamodb_sizing_script_macos.sh [options]
```

#### Options
- `-h, --help`     Show help message
- `-pre PREFIX`    Filter tables by prefix
- `-post POSTFIX`  Filter tables by postfix
- `-both`          Use both prefix and postfix filters
- `-all`           Process all tables (ignore filters)
- `-p PROFILE`     AWS profile to use
- `-a ACCOUNT`     AWS account number
- `-d DAYS`        Number of days to analyze (default: 7)

#### Examples
```bash
# Process tables with 'dev-' prefix and '-prod' suffix
./dynamodb_sizing_script.sh -pre dev- -post -prod

# Process all tables with custom profile
./dynamodb_sizing_script.sh -all -p myprofile -a 123456789012

# Analyze last 45 days for test tables
./dynamodb_sizing_script.sh -pre test- -d 45
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
| 4. Process Time  |
|    Windows       |
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

### Metrics Collection Scripts
The metrics collection scripts create a `dynamo_metrics_logs` directory containing:

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
    ├── DeleteItem/
    │   ├── sample_count/
    │   │   └── DeleteItem_SampleCount_*.log
    │   └── p99_latency/
    │       └── *.log
    └── Consolidated Files/
        ├── {table_name}_GetItem_sample_count-3hr.log
        ├── {table_name}_GetItem_p99_latency-3hr.log
        ├── {table_name}_GetItem_sample_count-7day.log
        ├── {table_name}_GetItem_p99_latency-7day.log
        └── ... (similar files for all operations)
```

<<<<<<< HEAD
### Sizing Scripts
The sizing scripts create a `logs` directory containing:

```
logs/
├── dynamodb_sizing_YYYYMMDD_HHMMSS.log
└── {table_name}_analysis/
    ├── table_details.json
    ├── metrics_summary.txt
    ├── capacity_analysis.txt
    └── scylla_migration_guide.txt
```
=======
**Key Features:**
- **Raw Data**: Individual log files for each time window and operation
- **Consolidated Data**: Combined files for each collection period (3hr/7day)
- **Preserved Raw Files**: Original data files are kept for detailed analysis
- **AWS Call Tracking**: Total API calls made during execution
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)

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
- AWS API call counter with debug logging

### Metrics Collection
- **Sample Counts**: Number of successful requests for each operation
- **P99 Latency**: 99th percentile latency measurements
<<<<<<< HEAD
- **Time Windows**: Multiple time periods (3 hours, 24 hours, 7 days, 30 days)
- **Operations**: GetItem, Query, Scan, PutItem, UpdateItem, DeleteItem

### Sizing Analysis
- **Capacity Planning**: Detailed analysis for ScyllaDB migration
- **Performance Metrics**: Comprehensive performance analysis
- **Cost Analysis**: Cost comparison between DynamoDB and ScyllaDB
- **Migration Guidance**: Step-by-step migration recommendations

### Performance Optimizations
- Background processing for parallel AWS calls
- Rate limiting with configurable thresholds
- Efficient file organization by table and operation
- Sequential processing to avoid API limits
=======
- **Dual Collection Periods**: 
  - 3-hour collection with 1-second granularity (20-minute intervals)
  - 7-day collection with 60-second granularity (24-hour intervals)
- **Operations**: GetItem, Query, Scan, PutItem, UpdateItem, DeleteItem
- **Consolidated Output**: Combined files for each collection period
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)

## Key Functions

### Metrics Collection Scripts

#### `get_sample_counts`
- Collects SampleCount statistics for all operations
- Processes read and write operations separately
- Creates organized directory structure
- Background processing for parallel execution
- Accepts configurable period parameter for different collection windows

#### `get_p99_latency`
- Collects P99 latency measurements
- Uses extended statistics for percentile data
- Separate processing for read/write operations
- Timestamp-based file naming
- Accepts configurable period parameter for different collection windows

<<<<<<< HEAD
=======
#### `consolidate_table_logs`
- Consolidates all raw log files for each collection period
- Creates combined files for 3-hour and 7-day periods
- Preserves raw files for detailed analysis
- Processes all tables and operations at once

>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)
#### `check_aws_credentials`
- Validates AWS credentials and permissions
- Supports AWS profiles
- EC2 instance profile detection
- Comprehensive error reporting

#### `get_default_region`
- Multiple region detection methods
- Environment variable support
- AWS CLI configuration fallback
- Instance metadata support

### Sizing Scripts

#### `setup_aws_config`
- Configures AWS credentials and profile
- Validates account access
- Sets up region configuration

#### `analyze_table_capacity`
- Analyzes table capacity requirements
- Calculates read/write capacity units
- Provides ScyllaDB sizing recommendations

#### `generate_migration_plan`
- Creates detailed migration plan
- Includes cost analysis
- Provides step-by-step guidance

## Performance Considerations

<<<<<<< HEAD
1. **Time Windows**: Multiple time periods for comprehensive analysis
2. **Period**: Configurable granularity (1 second to 1 hour)
=======
1. **Dual Collection Periods**: 3-hour and 7-day analysis for comprehensive coverage
2. **Period Granularity**: 1-second for detailed analysis, 60-second for long-term trends
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)
3. **Parallel Processing**: Background execution for AWS calls
4. **AWS Call Tracking**: Global counter for accurate API usage monitoring
5. **File Organization**: Structured output with consolidated files
6. **Raw File Preservation**: Original data kept for detailed analysis

## Error Handling

- AWS credential verification
- Table existence and access checks
- CloudWatch API error handling
- Region validation
- Comprehensive logging of all operations
- Graceful failure handling with detailed error messages
- AWS call counter debugging for troubleshooting

## Platform Differences

### Linux/Unix Versions
- Uses `date -d` for time calculations
- Standard Linux date command syntax
- Full multi-region support

### macOS Versions
- Uses `date -v` for time calculations
- macOS-compatible date command syntax
- Same functionality as Linux versions

## Use Cases

### Metrics Collection Scripts
<<<<<<< HEAD
- **Performance Monitoring**: Track table performance over time
- **Capacity Planning**: Understand current usage patterns
- **Troubleshooting**: Identify performance bottlenecks
- **Baseline Establishment**: Create performance baselines

### Sizing Scripts
- **Migration Planning**: Plan DynamoDB to ScyllaDB migration
- **Capacity Analysis**: Understand resource requirements
- **Cost Optimization**: Compare costs between platforms
- **Performance Optimization**: Identify optimization opportunities

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
10. **ScyllaDB Migration Support**: Added sizing scripts for migration planning
11. **Platform Compatibility**: Added macOS-specific versions
12. **Basic Analysis Option**: Simplified 7-day analysis script
=======
- **Performance Monitoring**: Track table performance over time with dual granularity
- **Capacity Planning**: Understand current usage patterns across different time scales
- **Troubleshooting**: Identify performance bottlenecks with detailed metrics
- **Baseline Establishment**: Create performance baselines for both short and long-term analysis
- **Migration Preparation**: Collect comprehensive data for ScyllaDB migration planning

## Recent Updates

1. **Dual Collection Periods**: Added 3-hour and 7-day collection windows
2. **AWS Call Tracking**: Global counter for accurate API usage monitoring
3. **Consolidated Output**: Combined files for each collection period
4. **Raw File Preservation**: Original data files kept for detailed analysis
5. **Enhanced Logging**: Comprehensive logging with AWS call debugging
6. **Multi-region Support**: Process tables across multiple AWS regions
7. **AWS Profile Support**: Use specific AWS profiles for authentication
8. **Improved Error Handling**: Better error messages and recovery
9. **Background Processing**: Parallel AWS calls for improved performance
10. **Structured Output**: Organized file structure for easy analysis
11. **Table Filtering**: Process specific tables or all tables
12. **Comprehensive Metrics**: Sample counts and P99 latency for all operations
13. **Platform Compatibility**: Added macOS-specific versions
>>>>>>> bb5449f (Update README.md with comprehensive documentation for all scripts)

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.