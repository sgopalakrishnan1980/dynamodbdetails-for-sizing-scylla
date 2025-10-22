# DynamoDB Metrics Collection Shell Scripts

> **📋 Shell Scripts Documentation** - Comprehensive guide for Linux and macOS versions of the DynamoDB metrics collection tool.

This repository provides two shell script implementations for collecting DynamoDB metrics from CloudWatch:

- **`dynamo_metrics_collection.sh`** - Linux/Unix version
- **`dynamo_metrics_collection_mac.sh`** - macOS optimized version

## 🚀 Quick Start

### Prerequisites

Before running either script, ensure your environment meets these requirements:

```bash
# Test system requirements
bash --version
aws --version

# MANDATORY: Run pre-execution checks
aws sts get-caller-identity
aws configure get region
aws dynamodb list-tables --max-items 1
aws cloudwatch list-metrics --namespace AWS/DynamoDB --max-items 1
```

> **⚠️ Important**: The script will fail if AWS authentication or region configuration is not properly set up. See the [Mandatory Pre-Execution Checks](#mandatory-pre-execution-checks) section for detailed requirements.

### Basic Usage

```bash
# Linux/Unix
./dynamo_metrics_collection.sh

# macOS
./dynamo_metrics_collection_mac.sh

# With specific options
./dynamo_metrics_collection.sh -t table1,table2 -r us-east-1 -p my-profile
```

## 📋 Mandatory Environment Checks

### 1. System Requirements

```bash
# Check bash version (required)
bash --version

# Verify script permissions
chmod +x dynamo_metrics_collection.sh
chmod +x dynamo_metrics_collection_mac.sh

# Test script syntax
bash -n dynamo_metrics_collection.sh
bash -n dynamo_metrics_collection_mac.sh
```

#### Environment Variables

The scripts use these environment variables (optional):

```bash
# AWS Configuration
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_SESSION_TOKEN="your-session-token"  # For temporary credentials
export AWS_PROFILE="your-profile-name"

# Script-specific variables (set automatically)
export LOG_DIR="dynamo_metrics_logs_$(date +%m%d%y%H%M%S)"
export PERIOD=1
export USE_INSTANCE_PROFILE=false
export TOTAL_AWS_CALLS=0
export WAIT_AWS_CALLS=0
```

**Required Environment Variables:**
- None (scripts can run with AWS CLI configuration only)

**Optional Environment Variables:**
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key  
- `AWS_DEFAULT_REGION` - Default AWS region
- `AWS_SESSION_TOKEN` - Session token for temporary credentials
- `AWS_PROFILE` - AWS profile name to use

#### Mandatory Pre-Execution Checks

**⚠️ REQUIRED: Run these commands before executing the script**

```bash
# 1. Test AWS authentication and get caller identity
aws sts get-caller-identity

# Expected output example:
# {
#     "UserId": "AIDACKCEVSQ6C2EXAMPLE",
#     "Account": "123456789012",
#     "Arn": "arn:aws:iam::123456789012:user/YourUsername"
# }

# 2. Verify default region is set
aws configure get region

# Expected output example:
# us-east-1

# 3. Test DynamoDB access in the region
aws dynamodb list-tables --max-items 1

# Expected output example:
# TABLENAMES    my-table-1    my-table-2

# 4. Test CloudWatch access
aws cloudwatch list-metrics --namespace AWS/DynamoDB --max-items 1

# Expected output example:
# METRICS       SuccessfulRequestLatency    AWS/DynamoDB
```

**If any of these commands fail:**
- **STS call fails**: Configure AWS credentials using `aws configure` or environment variables
- **No default region**: Set region using `aws configure set region us-east-1` or `export AWS_DEFAULT_REGION=us-east-1`
- **DynamoDB access fails**: Check IAM permissions for `dynamodb:ListTables`
- **CloudWatch access fails**: Check IAM permissions for `cloudwatch:ListMetrics`

### 2. Required System Tools

Both scripts require these standard Unix utilities:

```bash
# Core utilities
which date mkdir echo tee sed awk grep find sort cat curl jq

# Test date functionality (critical for time calculations)
date -u +"%Y-%m-%dT%H:%M:%SZ"
date -u -d "20 minutes ago" +"%Y-%m-%dT%H:%M:%SZ"  # Linux
date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ"              # macOS
```

### 3. AWS CLI Configuration

#### Installation
```bash
# Install AWS CLI (if not already installed)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Or using package manager
# Ubuntu/Debian
sudo apt-get install awscli

# macOS
brew install awscli
```

#### Configuration Methods

**Method 1: AWS Configure**
```bash
aws configure
# Enter: Access Key ID, Secret Access Key, Default region, Output format
```

**Method 2: Environment Variables**
```bash
# Set environment variables (see System Requirements section for full list)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_SESSION_TOKEN="your-session-token"  # If using temporary credentials
```

**Method 3: AWS Profiles**
```bash
aws configure --profile my-profile
export AWS_PROFILE="my-profile"
```

**Method 4: EC2 Instance Profile (for EC2 instances)**
```bash
# No configuration needed - uses instance metadata
./dynamo_metrics_collection.sh -I
```

### 4. AWS Permissions Validation

```bash
# Test required permissions
aws sts get-caller-identity
aws dynamodb list-tables --max-items 1
aws cloudwatch list-metrics --namespace AWS/DynamoDB --max-items 1

# Test specific regions
aws dynamodb list-tables --region us-east-1 --max-items 1
aws dynamodb list-tables --region us-west-2 --max-items 1
```

### 5. Network Connectivity

```bash
# Test internet connectivity
ping -c 3 8.8.8.8

# Test AWS endpoints
curl -s --connect-timeout 10 https://sts.amazonaws.com/ > /dev/null
curl -s --connect-timeout 10 https://dynamodb.us-east-1.amazonaws.com/ > /dev/null
curl -s --connect-timeout 10 https://monitoring.us-east-1.amazonaws.com/ > /dev/null
```

## 🔧 Script-Specific Requirements

### Linux Script (`dynamo_metrics_collection.sh`)

**Key Differences:**
- Uses GNU `date` command with `-d` flag for date arithmetic
- Standard Linux utilities and paths
- Compatible with most Linux distributions

**Date Command Examples:**
```bash
# 20 minutes ago
date -u -d "20 minutes ago" +"%Y-%m-%dT%H:%M:%SZ"

# 24 hours ago
date -u -d "24 hours ago" +"%Y-%m-%dT%H:%M:%SZ"
```

### macOS Script (`dynamo_metrics_collection_mac.sh`)

**Key Differences:**
- Uses BSD `date` command with `-v` flag for date arithmetic
- macOS-specific date syntax
- Optimized for macOS environment

**Date Command Examples:**
```bash
# 20 minutes ago
date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ"

# 24 hours ago
date -u -v-24H +"%Y-%m-%dT%H:%M:%SZ"
```

## 📊 Performance Estimates

### AWS API Calls Per Table

Each table requires the following API calls:

| Operation | 3-Hour Collection | 7-Day Collection | Total per Table |
|-----------|-------------------|------------------|-----------------|
| **List Tables** | 1 | 0 | 1 |
| **Describe Table** | 1 | 0 | 1 |
| **Sample Count (7 operations × 9 iterations)** | 63 | 49 | 112 |
| **P99 Latency (7 operations × 9 iterations)** | 63 | 49 | 112 |
| **Total per Table** | **128** | **98** | **226** |

**Operations Covered:**
- Read: GetItem, Query, Scan
- Write: PutItem, UpdateItem, DeleteItem, BatchWriteItem

### Time Estimates by Table Count

| Tables | Total API Calls | Estimated Time | Notes |
|--------|----------------|----------------|-------|
| **5 tables** | 1,130 calls | **8-12 minutes** | Fast execution |
| **10 tables** | 2,260 calls | **15-25 minutes** | Moderate load |
| **15 tables** | 3,390 calls | **25-40 minutes** | Higher load |
| **25 tables** | 5,650 calls | **45-75 minutes** | Heavy load |

**Time Factors:**
- **Network latency** to AWS endpoints
- **AWS API rate limits** (CloudWatch: 400 requests/second)
- **Background process management** (wait every 1000 calls)
- **File I/O operations** for log writing
- **System resources** (CPU, memory)

### Rate Limiting Behavior

The scripts implement intelligent rate limiting:

```bash
# Wait threshold: 1000 AWS calls
if [ $WAIT_AWS_CALLS -ge 1000 ]; then
    wait  # Wait for all background processes
    WAIT_AWS_CALLS=0  # Reset counter
fi
```

**Impact on Performance:**
- **5 tables**: No waits needed (1,130 < 1,000)
- **10 tables**: 2 wait cycles (2,260 ÷ 1,000 = 2.26)
- **15 tables**: 3 wait cycles (3,390 ÷ 1,000 = 3.39)
- **25 tables**: 5 wait cycles (5,650 ÷ 1,000 = 5.65)

## 🛠️ Usage Examples

### Basic Usage

```bash
# Process all tables in default region
./dynamo_metrics_collection.sh

# Process specific tables
./dynamo_metrics_collection.sh -t mytable1,mytable2

# Process tables in specific regions
./dynamo_metrics_collection.sh -r us-east-1,us-west-2

# Use specific AWS profile
./dynamo_metrics_collection.sh -p production

# Use EC2 instance profile
./dynamo_metrics_collection.sh -I
```

### Advanced Usage

```bash
# Multiple tables in multiple regions with profile
./dynamo_metrics_collection.sh \
  -t table1,table2,table3 \
  -r us-east-1,us-west-2,eu-west-1 \
  -p staging

# macOS version with specific configuration
./dynamo_metrics_collection_mac.sh \
  -t production-table \
  -r us-east-1 \
  -p production
```

## 📁 Output Structure

```
dynamo_metrics_logs_MMDDYYHHMMSS/
├── script_execution_YYYYMMDD_HHMMSS.log
├── table_detailed.log
└── region_name/
    └── table_name/
        ├── GetItem/
        │   ├── sample_count/
        │   └── p99_latency/
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

## 🔍 Troubleshooting

### Common Issues

**1. AWS Credentials Not Found**
```bash
# Check credentials
aws configure list
aws sts get-caller-identity

# Fix: Configure AWS CLI
aws configure
```

**2. Permission Denied**
```bash
# Check permissions
aws dynamodb list-tables
aws cloudwatch list-metrics --namespace AWS/DynamoDB

# Fix: Update IAM permissions
```

**3. Date Command Errors (macOS)**
```bash
# Test date command
date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ"

# Fix: Use macOS script version
./dynamo_metrics_collection_mac.sh
```

**4. Script Not Executable**
```bash
# Fix permissions
chmod +x dynamo_metrics_collection.sh
chmod +x dynamo_metrics_collection_mac.sh
```

### Debug Mode

Enable detailed logging by checking the log files:

```bash
# Monitor script execution
tail -f dynamo_metrics_logs_*/script_execution_*.log

# Check specific table processing
grep "table_name" dynamo_metrics_logs_*/script_execution_*.log
```

## 📋 Complete Environment Test Script

Save this as `test_environment.sh` and run before using the scripts:

```bash
#!/bin/bash
echo "=== DynamoDB Metrics Collection Environment Test ==="
echo "Date: $(date)"
echo "Hostname: $(hostname)"
echo "User: $(whoami)"
echo ""

echo "1. System Requirements:"
echo "  Bash version: $(bash --version | head -1)"
echo "  OS: $(uname -a)"
echo ""

echo "2. Required Tools:"
for tool in date mkdir echo tee sed awk grep find sort cat curl jq; do
    if command -v $tool >/dev/null 2>&1; then
        echo "  ✓ $tool: $(which $tool)"
    else
        echo "  ✗ $tool: NOT FOUND"
    fi
done
echo ""

echo "3. AWS CLI:"
if command -v aws >/dev/null 2>&1; then
    echo "  ✓ AWS CLI: $(aws --version)"
else
    echo "  ✗ AWS CLI: NOT FOUND"
fi
echo ""

echo "4. AWS Configuration:"
aws configure list
echo ""

echo "5. AWS Authentication:"
if aws sts get-caller-identity >/dev/null 2>&1; then
    echo "  ✓ AWS credentials valid"
    aws sts get-caller-identity
else
    echo "  ✗ AWS credentials invalid"
fi
echo ""

echo "6. AWS Default Region:"
if aws configure get region >/dev/null 2>&1; then
    echo "  ✓ Default region: $(aws configure get region)"
else
    echo "  ✗ Default region not configured"
fi
echo ""

echo "7. AWS Permissions:"
if aws dynamodb list-tables --max-items 1 >/dev/null 2>&1; then
    echo "  ✓ DynamoDB access: OK"
else
    echo "  ✗ DynamoDB access: FAILED"
fi

if aws cloudwatch list-metrics --namespace AWS/DynamoDB --max-items 1 >/dev/null 2>&1; then
    echo "  ✓ CloudWatch access: OK"
else
    echo "  ✗ CloudWatch access: FAILED"
fi
echo ""

echo "8. Script Syntax:"
if bash -n dynamo_metrics_collection.sh; then
    echo "  ✓ Linux script syntax: OK"
else
    echo "  ✗ Linux script syntax: FAILED"
fi

if bash -n dynamo_metrics_collection_mac.sh; then
    echo "  ✓ macOS script syntax: OK"
else
    echo "  ✗ macOS script syntax: FAILED"
fi
echo ""

echo "=== Environment Test Complete ==="
```

## 🔐 Required AWS Permissions

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

## 📈 Performance Optimization Tips

1. **Use specific regions** to reduce API calls
2. **Process tables in batches** if you have many tables
3. **Monitor AWS CloudWatch quotas** to avoid throttling
4. **Use EC2 instance profiles** when running on EC2
5. **Ensure stable network connectivity** to AWS endpoints

## 🆚 Script Comparison

| Feature | Linux Script | macOS Script |
|---------|--------------|--------------|
| **Date Commands** | GNU `date -d` | BSD `date -v` |
| **Compatibility** | Linux/Unix | macOS |
| **Performance** | Identical | Identical |
| **Output Format** | Identical | Identical |
| **API Calls** | Identical | Identical |

Both scripts provide identical functionality with platform-specific optimizations for date handling.
