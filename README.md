# DynamoDB Sizing and Metrics Collection Script

## Overview
This script collects comprehensive sizing and usage metrics for Amazon DynamoDB tables. It provides detailed information about table sizes, capacity utilization, and performance metrics to help with capacity planning, cost optimization, and performance analysis. The script is available in both Linux and macOS versions.

## Features
- Collects average item size, total table size, and provisioned capacity for each table
- Gathers CloudWatch metrics for consumed capacity units and request rates
- Captures P99 latency metrics for read and write operations
- Identifies and documents DynamoDB Streams configuration
- Captures information about Local Secondary Indexes (LSIs) and Global Secondary Indexes (GSIs)
- Supports filtering tables by prefix, postfix, or both
- Works with IAM roles and profiles for authentication
- Outputs data in both CSV and JSON formats for easy analysis
- Precise decimal handling for all numeric calculations
- Cross-platform compatibility (Linux and macOS)

## Prerequisites
- AWS CLI v2 installed and configured
- `jq` command-line JSON processor
- `bc` command-line calculator
- Appropriate AWS permissions to access:
  - DynamoDB tables (ListTables, DescribeTable)
  - CloudWatch metrics related to DynamoDB
  - STS (GetCallerIdentity)

## Installation
1. Download the appropriate script for your platform:
   ```bash
   # For Linux
   curl -O https://github.com/sgopalakrishnan1980/dynamodbdetails-for-sizing-scylla/blob/main/dynamodb_sizing_script.sh
   
   # For macOS
   curl -O https://github.com/sgopalakrishnan1980/dynamodbdetails-for-sizing-scylla/blob/main/dynamodb_sizing_script_macos.sh
   ```

2. Make the script executable:
   ```bash
   # For Linux
   chmod +x dynamodb_sizing_script.sh
   
   # For macOS
   chmod +x dynamodb_sizing_script_macos.sh
   ```

## Usage
Basic usage with default AWS profile:
```bash
# For Linux
./dynamodb_sizing_script.sh [options]

# For macOS
./dynamodb_sizing_script_macos.sh [options]
```

### Command-Line Options
```
Options:
  -pre PREFIX    Filter tables that start with PREFIX
  -post POSTFIX  Filter tables that end with POSTFIX
  -both          Use both prefix and postfix filters (AND condition)
  -all           Process all tables (overrides other filters)
  -a ACCOUNT     AWS account number (optional)
  -p PROFILE     AWS profile to use (optional)
  -h, --help     Display help information
```

### Examples

Process all tables using the default AWS profile:
```bash
# For Linux
./dynamodb_sizing_script.sh -all

# For macOS
./dynamodb_sizing_script_macos.sh -all
```

Process only tables starting with "users":
```bash
# For Linux
./dynamodb_sizing_script.sh -pre users

# For macOS
./dynamodb_sizing_script_macos.sh -pre users
```

Process only tables ending with "-prod":
```bash
# For Linux
./dynamodb_sizing_script.sh -post -prod

# For macOS
./dynamodb_sizing_script_macos.sh -post -prod
```

Process tables that match both criteria (starting with "users" AND ending with "-prod"):
```bash
# For Linux
./dynamodb_sizing_script.sh -pre users -post -prod -both

# For macOS
./dynamodb_sizing_script_macos.sh -pre users -post -prod -both
```

Use a specific AWS profile:
```bash
# For Linux
./dynamodb_sizing_script.sh -all -p my-profile

# For macOS
./dynamodb_sizing_script_macos.sh -all -p my-profile
```

Specify an account number manually:
```bash
# For Linux
./dynamodb_sizing_script.sh -all -a 123456789012

# For macOS
./dynamodb_sizing_script_macos.sh -all -a 123456789012
```

## Platform-Specific Notes

### Linux Version
- Uses standard Linux date command syntax
- Compatible with most Linux distributions
- Uses standard bash arithmetic operations

### macOS Version
- Uses macOS-specific date command syntax with `-v` flag
- Compatible with macOS 10.12 and later
- Uses `bc` for precise decimal arithmetic
- Maintains 2 decimal places for all numeric calculations

## Authentication Methods

### Using AWS Profiles
The script can use AWS profiles configured in your `~/.aws/credentials` or `~/.aws/config` files:
```bash
# For Linux
./dynamodb_sizing_script.sh -p my-profile -all

# For macOS
./dynamodb_sizing_script_macos.sh -p my-profile -all
```

### Using IAM Roles
When running on an EC2 instance with an instance profile or in an ECS task with a task role, no additional configuration is needed:
```bash
# For Linux
./dynamodb_sizing_script.sh -all

# For macOS
./dynamodb_sizing_script_macos.sh -all
```

### Using Environment Variables
You can set AWS credentials as environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_SESSION_TOKEN=your_session_token  # if using temporary credentials

# For Linux
./dynamodb_sizing_script.sh -all

# For macOS
./dynamodb_sizing_script_macos.sh -all
```

## Output Files
The script creates a timestamped directory with multiple output files:

- `dynamodb_summary.csv` - Summary CSV with key metrics for all tables
- `dynamodb_detailed.json` - Detailed JSON with full metrics for all tables
- `[TABLE_NAME]_CONSUMED_RCU` - Time-series data for consumed read capacity
- `[TABLE_NAME]_CONSUMED_WCU` - Time-series data for consumed write capacity
- `[TABLE_NAME]_GETITEM_HISTORY` - GetItem operation history
- `[TABLE_NAME]_PUTITEM_HISTORY` - PutItem operation history
- `[TABLE_NAME]_GETITEM_READP99_HISTORY` - P99 latency for read operations
- `[TABLE_NAME]_PUTITEM_WRITEP99_HISTORY` - P99 latency for write operations
- `[TABLE_NAME]_gsi_details.json` - GSI configuration for each table
- `[TABLE_NAME]_lsi_details.json` - LSI configuration for each table
- `script.log` - Execution log with filter and configuration details

## Numeric Precision
The script maintains precise decimal calculations for all metrics:
- All numeric calculations use `bc` with `scale=2` for 2 decimal places
- Average item sizes are calculated with 2 decimal places
- Table sizes are reported in GB with 2 decimal places
- P99 latencies are maintained with 2 decimal places
- All rate calculations (reads/sec, writes/sec) maintain 2 decimal places

## Troubleshooting

### Permission Issues
If the script fails with authentication errors:
1. Verify your AWS credentials have the necessary permissions
2. For IAM roles, ensure the role has permissions for:
   - `dynamodb:ListTables`
   - `dynamodb:DescribeTable`
   - `cloudwatch:GetMetricStatistics`
   - `sts:GetCallerIdentity`

### Missing Dependencies
If you encounter command not found errors:
1. Make sure `jq` is installed:
   - Linux: `apt-get install jq` or `yum install jq`
   - macOS: `brew install jq`
2. Make sure `bc` is installed:
   - Linux: `apt-get install bc` or `yum install bc`
   - macOS: `brew install bc`

### AWS CLI Version
This script requires AWS CLI v2. Check your version with:
```bash
aws --version
```

If using v1, upgrade to v2 following AWS documentation.

### Error Codes
- If you see "UnrecognizedClientException", your IAM credentials are invalid
- If you see "AccessDeniedException", your IAM role lacks required permissions

## Advanced Usage

### Analyzing Multiple Accounts
To analyze tables across multiple AWS accounts, run the script multiple times with different profiles:

```bash
# For Linux
./dynamodb_sizing_script.sh -all -p account1-profile -a 111111111111
./dynamodb_sizing_script.sh -all -p account2-profile -a 222222222222

# For macOS
./dynamodb_sizing_script_macos.sh -all -p account1-profile -a 111111111111
./dynamodb_sizing_script_macos.sh -all -p account2-profile -a 222222222222
```

### Debugging the Script
For debugging, the script already includes debug mode with `-x` flag. You can see detailed execution output as the script runs.

## License
This script is provided as-is with no warranty. Use at your own risk.

## Contributing
Contributions, suggestions, and bug reports are welcome.

## Development
Several parts of this script were generated and iteratively refined through prompt engineering sessions with Claude 3.7 Sonnet LLM. The development process incorporated feedback and testing to optimize the script's functionality across various AWS environments.
