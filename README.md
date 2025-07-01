# DynamoDB Metrics Collection Scripts

A comprehensive collection of scripts for analyzing DynamoDB tables and collecting performance metrics to assist with capacity planning and migration to ScyllaDB.

## Overview

This repository contains scripts designed to help with DynamoDB analysis and metrics collection:

### Metrics Collection Scripts
- **`dynamo_metrics_collection.sh`** (Linux/Unix) - Collects detailed CloudWatch metrics with dual collection periods
- **`dynamo_metrics_collection_mac.sh`** (macOS) - macOS-compatible metrics collection with dual collection periods

## Prerequisites

- AWS CLI installed and configured
- `jq` for JSON processing
- Bash shell environment
- AWS credentials with appropriate permissions for:
  - DynamoDB: `dynamodb:ListTables`, `dynamodb:DescribeTable`
  - CloudWatch: `cloudwatch:GetMetricStatistics`

## EC2 Instance Profile Authentication (-I Flag)

The scripts support EC2 Instance Profile authentication using the `-I` flag, which is particularly useful when running the scripts on EC2 instances or in containerized environments where traditional AWS credentials are not available or desired.

### How Instance Profile Authentication Works

When the `-I` flag is used, the script follows this execution logic:

1. **Metadata Token Retrieval**: 
   - Makes a PUT request to `http://169.254.169.254/latest/api/token` with a 6-hour TTL
   - This token is required for accessing EC2 instance metadata

2. **Instance Profile Detection**:
   - Retrieves the instance profile name from `http://169.254.169.254/latest/meta-data/iam/security-credentials/`
   - Validates that an instance profile is attached to the EC2 instance

3. **Credential Validation**:
   - Uses `aws sts get-caller-identity` to verify the instance profile is properly configured
   - Checks that the instance profile has the necessary permissions

4. **Error Handling**:
   - If not running on EC2, provides clear error messages
   - If no instance profile is attached, reports the issue
   - If permissions are insufficient, reports authentication failures

### Requirements for Instance Profile Authentication

#### EC2 Instance Requirements
- **EC2 Instance**: Must be running on an AWS EC2 instance
- **Instance Profile**: Must have an IAM role attached (instance profile)
- **Network Access**: Must have access to EC2 metadata service (169.254.169.254)
- **IAM Permissions**: The attached role must have the following permissions:
  ```json
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "dynamodb:ListTables",
          "dynamodb:DescribeTable",
          "cloudwatch:GetMetricStatistics",
          "sts:GetCallerIdentity"
        ],
        "Resource": "*"
      }
    ]
  }
  ```

#### Container Requirements (when running in containers)
- **EC2 Launch**: Container must be launched on an EC2 instance
- **Instance Profile**: The EC2 instance must have an instance profile attached
- **Metadata Access**: Container must have access to EC2 metadata service
- **No AWS Credentials**: Should not have AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY set

### Use Cases for Instance Profile Authentication

#### 1. EC2-Based Execution
```bash
# Running directly on EC2 instance with instance profile
./dynamo_metrics_collection.sh -I

# Running with specific regions and tables
./dynamo_metrics_collection.sh -I -r us-east-1,us-west-2 -t mytable
```

#### 2. Containerized Execution on EC2
```bash
# Running in Docker container on EC2 with instance profile
docker run -it --rm \
  -v $(pwd)/logs:/app/logs \
  dynamodb-metrics-collector \
  ./dynamo_metrics_collection.sh -I -r us-east-1
```

#### 3. Automated/CI-CD Pipelines
```bash
# In automated scripts where credentials are managed via instance profiles
./dynamo_metrics_collection.sh -I -r us-east-1 -t production-table
```

### Advantages of Instance Profile Authentication

1. **Security**: No need to store or pass AWS credentials
2. **Automatic Rotation**: IAM roles handle credential rotation automatically
3. **Least Privilege**: Can assign specific permissions to the instance profile
4. **Auditability**: All actions are tied to the specific EC2 instance and role
5. **Container Friendly**: Works seamlessly in containerized environments

### Troubleshooting Instance Profile Issues

#### Common Error Messages and Solutions

**Error: "Could not retrieve EC2 metadata token. Not running on EC2?"**
- **Cause**: Script is not running on an EC2 instance
- **Solution**: Run on an EC2 instance or use traditional AWS credentials

**Error: "Could not retrieve instance profile name from metadata. Not running on EC2 or no profile attached."**
- **Cause**: No IAM role is attached to the EC2 instance
- **Solution**: Attach an IAM role to the EC2 instance with appropriate permissions

**Error: "Instance profile not properly configured or not authorized"**
- **Cause**: The attached IAM role lacks necessary permissions
- **Solution**: Add the required DynamoDB and CloudWatch permissions to the IAM role

#### Verification Steps
```bash
# Check if running on EC2
curl -s http://169.254.169.254/latest/meta-data/instance-id

# Check instance profile
curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/

# Test AWS authentication
aws sts get-caller-identity
```

### Comparison: Instance Profile vs Traditional Credentials

| Aspect | Instance Profile (-I) | Traditional Credentials |
|--------|----------------------|------------------------|
| **Setup** | Requires EC2 + IAM role | Requires AWS CLI configuration |
| **Security** | No credential storage | Credentials stored locally |
| **Rotation** | Automatic | Manual |
| **Portability** | EC2-specific | Works anywhere |
| **Container Support** | Native | Requires credential mounting |
| **CI/CD Integration** | Excellent | Good |

## Installation

### Option 1: Direct Installation
1. Clone the repository
2. Make the scripts executable:
```bash
chmod +x *.sh
```

### Option 2: Docker Container (Work in Progress)
A Docker container is available for running the scripts in an isolated environment.

#### Building the Container

**Using Docker:**
```bash
docker build -t dynamodb-metrics-collector .
```

**Using Podman:**
```bash
podman build -t dynamodb-metrics-collector .
```

#### Running the Container

**Using Docker:**
```bash
docker run -it --rm dynamodb-metrics-collector
```

**Using Podman:**
```bash
podman run -it --rm dynamodb-metrics-collector
```

#### AWS Configuration in Container

Once inside the container, you'll need to configure AWS credentials. Choose one of the following methods:

**Method 1: AWS Configure (Interactive)**
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Enter your default region (e.g., us-east-1)
# Enter your default output format (json)
```

**Method 2: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID="your_access_key_id"
export AWS_SECRET_ACCESS_KEY="your_secret_access_key"
export AWS_SESSION_TOKEN="your_session_token"  # If using temporary credentials
export AWS_DEFAULT_REGION="us-east-1"
```

**Method 3: AWS Profile**
```bash
aws configure --profile myprofile
# Then use: aws configure list-profiles
# And set: export AWS_PROFILE="myprofile"
```

#### Running Scripts in Container

After configuring AWS credentials, you can run the scripts normally:
```bash
# Process all tables in current region
./dynamo_metrics_collection.sh

# Process specific table with custom profile
./dynamo_metrics_collection.sh -t mytable -p myprofile

# Process tables in specific regions
./dynamo_metrics_collection.sh -r us-east-1,us-west-2
```

#### Container with Volume Mount (Recommended)

To persist logs and data outside the container:

**Using Docker:**
```bash
docker run -it --rm \
  -v $(pwd)/logs:/app/dynamodbdetails/logs \
  -e AWS_ACCESS_KEY_ID="your_access_key_id" \
  -e AWS_SECRET_ACCESS_KEY="your_secret_access_key" \
  -e AWS_SESSION_TOKEN="your_session_token" \
  -e AWS_DEFAULT_REGION="us-east-1" \
  dynamodb-metrics-collector
```

**Using Podman:**
```bash
podman run -it --rm \
  -v $(pwd)/logs:/app/dynamodbdetails/logs \
  -e AWS_ACCESS_KEY_ID="your_access_key_id" \
  -e AWS_SECRET_ACCESS_KEY="your_secret_access_key" \
  -e AWS_SESSION_TOKEN="your_session_token" \
  -e AWS_DEFAULT_REGION="us-east-1" \
  dynamodb-metrics-collector
```

**Note:** The Docker support is currently a work in progress. Some features may not work as expected, and the container setup may require adjustments based on your specific environment and requirements.

## Script Categories

### Metrics Collection Scripts

These scripts collect detailed CloudWatch metrics for DynamoDB tables including sample counts and P99 latency measurements across two collection periods:
- **3-Hour Collection**: 9 iterations with 1-second period (20-minute intervals)
- **7-Day Collection**: 7 iterations with 60-second period (24-hour intervals)

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
- `-p <aws_profile>` Optional: AWS profile to use (cannot be used with `-I`)
- `-r <regions>`     Optional: Comma-separated list of regions to process. If not provided, uses current region.
- `-I`               Optional: Use EC2 Instance Profile for authentication (requires running on EC2 with attached IAM role)

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

# Use EC2 Instance Profile authentication (must run on EC2)
./dynamo_metrics_collection.sh -I

# Use Instance Profile with specific regions and tables
./dynamo_metrics_collection.sh -I -r us-east-1,us-west-2 -t mytable

# Container execution with Instance Profile
docker run -it --rm dynamodb-metrics-collector ./dynamo_metrics_collection.sh -I -r us-east-1
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
| - Check -I flag  |
| - Instance Profile|
|   or Traditional |
|   Credentials    |
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
    ├── BatchWriteItem/
    │   ├── sample_count/
    │   │   └── BatchWriteItem_SampleCount_*.log
    │   └── p99_latency/
    │       └── *.log
    └── Consolidated Files/
        ├── {table_name}_GetItem_sample_count-3hr.log
        ├── {table_name}_GetItem_p99_latency-3hr.log
        ├── {table_name}_GetItem_sample_count-7day.log
        ├── {table_name}_GetItem_p99_latency-7day.log
        └── ... (similar files for all operations)
```

**Key Features:**
- **Raw Data**: Individual log files for each time window and operation
- **Consolidated Data**: Combined files for each collection period (3hr/7day)
- **Preserved Raw Files**: Original data files are kept for detailed analysis
- **AWS Call Tracking**: Total API calls made during execution

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
- **Dual Collection Periods**: 
  - 3-hour collection with 1-second granularity (20-minute intervals)
  - 7-day collection with 60-second granularity (24-hour intervals)
- **Operations**: GetItem, Query, Scan, PutItem, UpdateItem, DeleteItem, BatchWriteItem
- **Consolidated Output**: Combined files for each collection period

### Performance Optimizations
- Background processing for parallel AWS calls
- Accurate AWS call tracking with global counter
- Efficient file organization by table and operation
- Sequential processing to avoid API limits
- Consolidated log processing after each collection period

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

#### `consolidate_table_logs`
- Consolidates all raw log files for each collection period
- Creates combined files for 3-hour and 7-day periods
- Preserves raw files for detailed analysis
- Processes all tables and operations at once

#### `check_aws_credentials`
- Validates AWS credentials and permissions
- Supports AWS profiles (when not using `-I` flag)
- EC2 instance profile detection and validation (when using `-I` flag)
- Comprehensive error reporting with specific guidance for each authentication method
- Handles metadata token retrieval for EC2 instance profiles
- Validates instance profile permissions and configuration

#### `get_default_region`
- Multiple region detection methods
- Environment variable support
- AWS CLI configuration fallback
- Instance metadata support

## Performance Considerations

1. **Dual Collection Periods**: 3-hour and 7-day analysis for comprehensive coverage
2. **Period Granularity**: 1-second for detailed analysis, 60-second for long-term trends
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
14. **EC2 Instance Profile Support**: Enhanced `-I` flag with detailed authentication logic and comprehensive documentation

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the GNU General Public License v3.0 - see the LICENSE file for details.