#!/bin/bash

# ============================================================================
# DynamoDB Metrics Collection Script (macOS Version)
# This script collects metrics from DynamoDB tables using CloudWatch
# ============================================================================

# Global variables
LOG_DIR="dynamo_metrics_logs_$(date +%m%d%y%H%M%S)"
AWS_PROFILE=""
PERIOD=1  # Default period for CloudWatch metrics in seconds
REGIONS_TO_PROBE=()  # Array to store regions to probe
TABLE_NAME=""  # Initialize TABLE_NAME as empty
TABLE_NAMES=() #inititalize arrays
USE_INSTANCE_PROFILE=false
# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Initialize log file
LOG_FILE="${LOG_DIR}/script_execution_$(date +%Y%m%d_%H%M%S).log"

# Arrays for operation types
READ_OPERATIONS=("GetItem" "Query" "Scan")
WRITE_OPERATIONS=("PutItem" "UpdateItem" "DeleteItem" "BatchWriteItem")

# Initialize variables
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
START_TIME=$(date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ")
START_TIME_FILENAME=$(echo "$START_TIME" | sed 's/:/_/g' | sed 's/T/_/g')
END_TIME_FILENAME=$(echo "$CURRENT_TIME" | sed 's/:/_/g' | sed 's/T/_/g')
default_region=$(aws configure get region $profile_arg 2>&1)

# Function to log messages with timestamp
log_message() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local message="$1"
    local level="${2:-INFO}"  # Default to INFO if level not specified
    
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Function to get default region
get_default_region() {
    local profile_arg=""
    if [ -n "$AWS_PROFILE" ]; then
        profile_arg="--profile $AWS_PROFILE"
    fi
    
    # First try AWS_DEFAULT_REGION environment variable
    if [ -n "$AWS_DEFAULT_REGION" ]; then
        log_message "Using region from AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION" "DEBUG" >&2
        echo "$AWS_DEFAULT_REGION"
        return 0
    fi
    
    # Then try AWS CLI configuration
    log_message "Trying to get region from AWS CLI configuration..." "DEBUG" >&2
    #local default_region
    default_region=$(aws configure get region $profile_arg 2>&1)
    local status=$?
    
    if [ $status -eq 0 ] && [ -n "$default_region" ]; then
        log_message "Found region in AWS CLI configuration: $default_region" "DEBUG" >&2
        echo "$default_region"
        return 0
    fi
    
    # If we get here, try to get region from instance metadata
 #   log_message "Trying to get region from instance metadata..." "DEBUG" >&2
  #  TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null)
 #   if [ -n "$TOKEN" ]; then
        default_region=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null)
 #       if [ -n "$default_region" ]; then
##            log_message "Found region from instance metadata: $default_region" "DEBUG" >&2
#            echo "$default_region"
#            return 0
#        fi
  #  fi
    
   # log_message "Could not determine default region" "ERROR" >&2
    #return 1
}

# Function to show usage information
show_usage() {
    echo "Usage: $0 [-t <table_name>] [-p <aws_profile>] [-r <regions>] [-I]"
    echo "Options:"
    echo "  -t <table_name>  Optional: Specific table to process. If not provided, all tables will be processed."
    echo "  -p <aws_profile> Optional: AWS profile to use"
    echo "  -r <regions>     Optional: Comma-separated list of regions to process. If not provided, uses current region."
    echo "  -I               Optional: Use EC2 Instance Profile for authentication"
    echo ""
    echo "Examples:"
    echo "  $0                    # Process all tables in current region"
    echo "  $0 -t mytable         # Process only 'mytable' in current region"
    echo "  $0 -r us-east-1       # Process all tables in us-east-1"
    echo "  $0 -t mytable -r us-east-1,us-west-2  # Process 'mytable' in specified regions"
    exit 1
}

# Parse command line arguments
while getopts "t:p:r:I" opt; do
    case $opt in
        t) TABLE_NAME="$OPTARG";;
        p) AWS_PROFILE="$OPTARG";;
        r) IFS=',' read -ra REGIONS_TO_PROBE <<< "$OPTARG";;
        I) USE_INSTANCE_PROFILE=true;;
        \?) show_usage;;
    esac
done

# Shift the processed options
shift $((OPTIND-1))

# Log script arguments
log_script_args() {
    local args_str=""
    if [ -n "$AWS_PROFILE" ]; then
        args_str="$args_str -p $AWS_PROFILE"
    fi
    if [ -n "$TABLE_NAME" ]; then
        args_str="$args_str -t $TABLE_NAME"
    fi
    if [ ${#REGIONS_TO_PROBE[@]} -gt 0 ]; then
        args_str="$args_str -r ${REGIONS_TO_PROBE[*]}"
    fi
    if [ "$USE_INSTANCE_PROFILE" = true ]; then
        args_str="$args_str -I"
    fi
    
    log_message "Script started with arguments: $args_str" "INFO"
    if [ -n "$TABLE_NAME" ]; then
        log_message "TABLE_NAME: $TABLE_NAME" "INFO"
    else
        log_message "No specific table specified, will process all tables" "INFO"
    fi
}

# Call log_script_args to log the current state
log_script_args

# Function to log function calls with arguments
log_function_call() {
    local function_name="$1"
    shift
    local args=("$@")
    local args_str=$(printf " '%s'" "${args[@]}")
    log_message "Function called: $function_name with arguments:$args_str" "DEBUG"
}

# If no regions specified, get default region
if [ ${#REGIONS_TO_PROBE[@]} -eq 0 ]; then
    log_message "No regions specified, getting default region..." "DEBUG"
    default_region=$(get_default_region)
    if [ $? -eq 0 ]; then
        REGIONS_TO_PROBE=("$default_region")
        log_message "Using default region: $default_region" "INFO"
    else
        log_message "Error: Could not determine default region. Please specify a region using -r option." "ERROR"
        show_usage
        exit 1
    fi
else
    log_message "Using specified regions: ${REGIONS_TO_PROBE[*]}" "INFO"
fi

# Function to check AWS credentials and identity
check_aws_credentials() {
    local profile_arg=""
    if [ "$USE_INSTANCE_PROFILE" = true ]; then
        log_message "Inferring credentials using EC2 Instance Profile..." "INFO"
        # Get the instance profile name
        TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null)
        if [ -n "$TOKEN" ]; then
            INSTANCE_PROFILE_NAME=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/iam/security-credentials/ 2>/dev/null)
            if [ -n "$INSTANCE_PROFILE_NAME" ]; then
                log_message "Using instance profile: $INSTANCE_PROFILE_NAME" "INFO"
                # Try to get caller identity with instance profile
                identity_info=$(aws sts get-caller-identity 2>/dev/null)
                if [ $? -ne 0 ]; then
                    log_message "Error: Instance profile not properly configured or not authorized" "ERROR"
                    return 1
                fi
            else
                log_message "Error: Could not retrieve instance profile name from metadata. Not running on EC2 or no profile attached." "ERROR"
                return 1
            fi
        else
            log_message "Error: Could not retrieve EC2 metadata token. Not running on EC2?" "ERROR"
            return 1
        fi
    else
        if [ -n "$AWS_PROFILE" ]; then
            profile_arg="--profile $AWS_PROFILE"
        fi
        # Try to get caller identity
        identity_info=$(aws sts get-caller-identity $profile_arg 2>/dev/null)
        if [ $? -ne 0 ]; then
            if [ -n "$AWS_PROFILE" ]; then
                if ! aws configure list-profiles | grep -q "^$AWS_PROFILE$"; then
                    log_message "Error: AWS profile '$AWS_PROFILE' not found" "ERROR"
                    return 1
                fi
            fi
            log_message "Error: AWS credentials not configured or invalid" "ERROR"
            return 1
        fi
    fi
    # Log the identity being used
    local account_id
    local arn
    account_id=$(echo "$identity_info" | jq -r '.Account')
    arn=$(echo "$identity_info" | jq -r '.Arn')
    log_message "Using AWS credentials for account: $account_id" "INFO"
    log_message "Identity: $arn" "INFO"
    return 0
}

# Function to get sample counts for all operations
get_sample_counts() {
    local table_name=$1
    local start_time=$2
    local current_time=$3
    local iteration=$4
    log_function_call "get_sample_counts" "$table_name" "$start_time" "$current_time" "$iteration"
    
    # Validate input parameter
    if [ -z "$table_name" ]; then
        log_message "Error: Table name is required for get_sample_counts" "ERROR"
        return 1
    fi
    
    # Create table directory if it doesn't exist
    local table_dir="${LOG_DIR}/${table_name}"
    mkdir -p "$table_dir"
    
    # Set profile argument if specified
    local profile_arg=""
    if [ -n "$AWS_PROFILE" ]; then
        profile_arg="--profile $AWS_PROFILE"
    fi
    
    # Process read operations
    for op in "${READ_OPERATIONS[@]}"; do
        log_message "Processing $op operation..." "INFO"
        
        # Create operation directory
        local op_dir="${table_dir}/${op}"
        mkdir -p "$op_dir"
        
        # Create metric directory
        local metric_dir="${op_dir}/sample_count"
        mkdir -p "$metric_dir"
        
        # Format timestamps for filename
        local start_time_filename=$(echo "$start_time" | sed 's/[:-]//g' | sed 's/T//g')
        local end_time_filename=$(echo "$current_time" | sed 's/[:-]//g' | sed 's/T//g')
        
        # Create log file with timestamps
        local log_file="${metric_dir}/${op}_SampleCount_${start_time_filename}to${end_time_filename}.log"
        log_message "Created metrics log file: $log_file" "INFO"
        
        # Construct the AWS CLI command
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name ${op}Latency --start-time \"$start_time\" --end-time \"$current_time\" --period $PERIOD --statistics SampleCount --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get metrics and append to log file in background
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name ${op}Latency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $PERIOD \
                --statistics SampleCount \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op" \
                --output text > "$log_file"
            echo "" >> "$log_file"
        ) &
    done
    
    # Process write operations
    for op in "${WRITE_OPERATIONS[@]}"; do
        log_message "Processing $op operation..." "INFO"
        
        # Create operation directory
        local op_dir="${table_dir}/${op}"
        mkdir -p "$op_dir"
        
        # Create metric directory
        local metric_dir="${op_dir}/sample_count"
        mkdir -p "$metric_dir"
        
        # Format timestamps for filename
        local start_time_filename=$(echo "$start_time" | sed 's/[:-]//g' | sed 's/T//g')
        local end_time_filename=$(echo "$current_time" | sed 's/[:-]//g' | sed 's/T//g')
        
        # Create log file with timestamps
        local log_file="${metric_dir}/${op}_SampleCount${start_time_filename}to${end_time_filename}.log"
        log_message "Created metrics log file: $log_file" "INFO"
        
        # Construct the AWS CLI command
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $PERIOD --statistics SampleCount --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get metrics and append to log file in background
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $PERIOD \
                --statistics SampleCount \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op"  \
                --output text > "$log_file"
            echo "" >> "$log_file"
        ) &
    done
    
    # Wait for all background processes to complete
    wait
    
    log_message "Sample counts collection completed for iteration $iteration" "INFO"
}

# Function to get P99 latency for all operations
get_p99_latency() {
    local table_name=$1
    local start_time=$2
    local current_time=$3
    local iteration=$4
    log_function_call "get_p99_latency" "$table_name" "$start_time" "$current_time" "$iteration"
    
    # Validate input parameter
    if [ -z "$table_name" ]; then
        log_message "Error: Table name is required for get_p99_latency" "ERROR"
        return 1
    fi
    
    # Create table directory if it doesn't exist
    local table_dir="${LOG_DIR}/${table_name}"
    mkdir -p "$table_dir"
    
    # Set profile argument if specified
    local profile_arg=""
    if [ -n "$AWS_PROFILE" ]; then
        profile_arg="--profile $AWS_PROFILE"
    fi
    
    # Process read operations
    for op in "${READ_OPERATIONS[@]}"; do
        log_message "Processing P99 latency for $op operation..." "INFO"
        
        # Create operation directory
        local op_dir="${table_dir}/${op}"
        mkdir -p "$op_dir"
        
        # Create metric directory
        local metric_dir="${op_dir}/p99_latency"
        mkdir -p "$metric_dir"
        
        # Format timestamps for filename
        local start_time_filename=$(echo "$start_time" | sed 's/[:-]//g' | sed 's/T//g')
        local end_time_filename=$(echo "$current_time" | sed 's/[:-]//g' | sed 's/T//g')
        
        # Create log file with timestamps
        local log_file="${metric_dir}/p99_${op}_${start_time_filename}to${end_time_filename}.log"
        
        # Construct the AWS CLI command
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $PERIOD --extended-statistics p99 --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get P99 metrics and write to log file in background
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $PERIOD \
                --extended-statistics p99 \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op" \
                --output text > "$log_file"
        ) &
        log_message "P99 latency metrics for $op will be saved to: $log_file" "INFO"
    done
    
    # Process write operations
    for op in "${WRITE_OPERATIONS[@]}"; do
        log_message "Processing P99 latency for $op operation..." "INFO"
        
        # Create operation directory
        local op_dir="${table_dir}/${op}"
        mkdir -p "$op_dir"
        
        # Create metric directory
        local metric_dir="${op_dir}/p99_latency"
        mkdir -p "$metric_dir"
        
        # Format timestamps for filename
        local start_time_filename=$(echo "$start_time" | sed 's/[:-]//g' | sed 's/T//g')
        local end_time_filename=$(echo "$current_time" | sed 's/[:-]//g' | sed 's/T//g')
        
        # Create log file with timestamps
        local log_file="${metric_dir}/${start_time_filename}to${end_time_filename}.log"
        
        # Construct the AWS CLI command
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name ${op}Latency --start-time \"$start_time\" --end-time \"$current_time\" --period $PERIOD --extended-statistics p99 --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get P99 metrics and write to log file in background
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name ${op}Latency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $PERIOD \
                --extended-statistics p99 \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op" \
                --output text > "$log_file"
        ) &
        log_message "P99 latency metrics for $op will be saved to: $log_file" "INFO"
    done
    
    # Wait for all background processes to complete
    wait
}

# Function to validate required variables
validate_variables() {
    log_function_call "validate_variables"
    
    # Check AWS credentials
    if ! check_aws_credentials; then
        exit 1
    fi
    
    log_message "Variables validated successfully" "INFO"
}

# Function to consolidate log files by table, API, and metric name
consolidate_table_logs() {
    local table_name=$1
    local region=$2
    
    log_message "Starting log consolidation for table: $table_name in region $region" "INFO"
    
    # Create table directory path
    local table_dir="${LOG_DIR}/${table_name}"
    
    if [ ! -d "$table_dir" ]; then
        log_message "Table directory not found: $table_dir" "WARN"
        return 1
    fi
    
    # Process each operation type (GetItem, Query, Scan, PutItem, etc.)
    for op in "${READ_OPERATIONS[@]}" "${WRITE_OPERATIONS[@]}"; do
        local op_dir="${table_dir}/${op}"
        
        if [ ! -d "$op_dir" ]; then
            log_message "Operation directory not found: $op_dir" "DEBUG"
            continue
        fi
        
        # Process sample_count metrics
        local sample_count_dir="${op_dir}/sample_count"
        if [ -d "$sample_count_dir" ]; then
            local consolidated_sample_count="${table_dir}/${table_name}_${op}_sample_count-3hrs.log"
            
            # Add header to consolidated file
            {
                echo "================================================"
                echo "TABLE: $table_name"
                echo "REGION: $region"
                echo "OPERATION: $op"
                echo "METRIC: SampleCount"
                echo "PERIOD: 3 hours (20-minute intervals)"
                echo "GENERATED: $(date)"
                echo "================================================"
                echo ""
            } > "$consolidated_sample_count"
            
            # Concatenate all sample count log files for this operation
            if find "$sample_count_dir" -name "*.log" -type f | grep -q .; then
                find "$sample_count_dir" -name "*.log" -type f | sort | while read -r log_file; do
                    echo "--- $(basename "$log_file") ---" >> "$consolidated_sample_count"
                    cat "$log_file" >> "$consolidated_sample_count"
                    echo "" >> "$consolidated_sample_count"
                done
                log_message "Created consolidated sample count log: $consolidated_sample_count" "INFO"
            else
                log_message "No sample count log files found for $op operation" "DEBUG"
            fi
        fi
        
        # Process p99_latency metrics
        local p99_latency_dir="${op_dir}/p99_latency"
        if [ -d "$p99_latency_dir" ]; then
            local consolidated_p99_latency="${table_dir}/${table_name}_${op}_p99_latency-3hrs.log"
            
            # Add header to consolidated file
            {
                echo "================================================"
                echo "TABLE: $table_name"
                echo "REGION: $region"
                echo "OPERATION: $op"
                echo "METRIC: P99 Latency"
                echo "PERIOD: 3 hours (20-minute intervals)"
                echo "GENERATED: $(date)"
                echo "================================================"
                echo ""
            } > "$consolidated_p99_latency"
            
            # Concatenate all P99 latency log files for this operation
            if find "$p99_latency_dir" -name "*.log" -type f | grep -q .; then
                find "$p99_latency_dir" -name "*.log" -type f | sort | while read -r log_file; do
                    echo "--- $(basename "$log_file") ---" >> "$consolidated_p99_latency"
                    cat "$log_file" >> "$consolidated_p99_latency"
                    echo "" >> "$consolidated_p99_latency"
                done
                log_message "Created consolidated P99 latency log: $consolidated_p99_latency" "INFO"
            else
                log_message "No P99 latency log files found for $op operation" "DEBUG"
            fi
        fi
    done
    
    log_message "Log consolidation completed for table: $table_name" "INFO"
    echo "================================================"
    echo "Log consolidation completed for table: $table_name"
    echo "Region: $region"
    echo "Consolidated files created in: $table_dir"
    echo "================================================"
}

# Main function to control script execution
main() {
    log_message "Starting main function..." "DEBUG"
    # Create a log file for table details
    local details_log="${LOG_DIR}/table_detailed.log"
    log_message "Creating table details log file: $details_log" "INFO"
    
    # Array to store all table names across regions
    declare -a tables_with_regions
    # Process each region in REGIONS_TO_PROBE
    for region in "${REGIONS_TO_PROBE[@]}"; do
        log_message "Processing region: $region" "INFO"
        local profile_arg=""
        if [ -n "$AWS_PROFILE" ]; then
            profile_arg="--profile $AWS_PROFILE"
        fi
        
        log_message "Using AWS profile: ${AWS_PROFILE:-default}" "INFO"
        
        # Get list of tables in the region
        log_message "Executing: aws dynamodb list-tables $profile_arg --region $region --output text" "DEBUG"
        local tables
        tables=$(aws dynamodb list-tables $profile_arg --region "$region" --output text 2>&1)
        
        # Process the table list to extract table names from the TABLENAMES format
        tables=$(echo "$tables" | awk '/^TABLENAMES/ {for(i=2; i<=NF; i++) print $i}' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        
        if [ -z "$tables" ]; then
            log_message "No tables found in region $region" "INFO"
            continue
        fi
        
        log_message "Found tables in region $region: $tables" "DEBUG"
        
        # Process each table name
        while IFS= read -r table_name; do
            # Skip empty lines and trim whitespace
            table_name=$(echo "$table_name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
            [[ -z "$table_name" ]] && continue
            
            # If TABLE_NAME is specified, only process that table
            if [ -n "$TABLE_NAME" ] && [ "$table_name" != "$TABLE_NAME" ]; then
                continue
            fi
            
            if [ -n "$table_name" ]; then
                log_message "Getting details for table: $table_name in region $region" "INFO"
                
                # Get table details
                log_message "Executing: aws dynamodb describe-table $profile_arg --region $region --table-name $table_name" "DEBUG"
                local table_details
                table_details=$(aws dynamodb describe-table $profile_arg --region "$region" --table-name "$table_name" --output table 2>&1)
                local describe_status=$?
                
                if [ $describe_status -eq 0 ]; then
                    # Add region information to the table details
                    local table_details_with_region
                    table_details_with_region=$(echo "$table_details" | sed "s/^/Region: $region | /")
                    
                    # Append to log file with a separator
                    echo "=== Table: $table_name (Region: $region) ===" >> "$details_log"
                    echo "$table_details_with_region" >> "$details_log"
                    echo "" >> "$details_log"
                    
                    # Add table name to the array with region information
                    tables_with_regions+=("$table_name:$region")
                    log_message "Added table $table_name from region $region to processing list" "INFO"
                else
                    log_message "Error: Failed to get details for table $table_name in region $region. AWS CLI returned: $table_details" "ERROR"
                fi
            fi
        done <<< "$tables"
    done
    
    if [ ${#tables_with_regions[@]} -eq 0 ]; then
        log_message "No tables found to process. Exiting." "ERROR"
        exit 1
    fi
    
    log_message "Found ${#tables_with_regions[@]} tables to process" "INFO"
    
    # Continue with existing metrics collection
    log_message "Starting DynamoDB metrics collection for last 3 hours in 20-minute intervals..." "INFO"
    
    # Initialize iteration counter and AWS call counter
    local iteration=1
    local max_iterations=9
    local aws_call_counter=0
    local max_calls_before_pause=100
    
    # Initialize time variables for the first iteration
    local current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local start_time=$(date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ")
    
    # Process each table
    for table_info in "${tables_with_regions[@]}"; do
        # Split table name and region
        IFS=':' read -r table_name region <<< "$table_info"
        
        # Skip if table name is empty
        if [ -z "$table_name" ]; then
            log_message "Skipping empty table name" "WARN"
            continue
        fi
        
        # Set the current table name
        TABLE_NAME="$table_name"
        
        log_message "Starting processing for table: $TABLE_NAME in region $region" "INFO"
        
        # Set AWS region for this table
        export AWS_DEFAULT_REGION="$region"
        
        while [ $iteration -le $max_iterations ]; do
            log_message "Starting iteration $iteration of $max_iterations for table $TABLE_NAME" "INFO"
            log_message "Time range: $start_time to $current_time" "INFO"
            
            # Validate environment and variables
            validate_variables
            
            # Get sample counts
            log_message "Collecting sample counts for iteration $iteration..." "INFO"
            if ! get_sample_counts "$TABLE_NAME" "$start_time" "$current_time" "$iteration"; then
                log_message "Error: Failed to collect sample counts for iteration $iteration" "ERROR"
                continue
            fi
            
            # Get P99 latency
            log_message "Collecting P99 latency metrics for iteration $iteration..." "INFO"
            if ! get_p99_latency "$TABLE_NAME" "$start_time" "$current_time" "$iteration"; then
                log_message "Error: Failed to collect P99 latency metrics for iteration $iteration" "ERROR"
                continue
            fi
            
            # Increment AWS call counter (each operation makes one call)
            aws_call_counter=$((aws_call_counter + ${#READ_OPERATIONS[@]} + ${#WRITE_OPERATIONS[@]}))
            
            # Check if we need to wait for background processes
            if [ $aws_call_counter -ge $max_calls_before_pause ]; then
                log_message "Reached $aws_call_counter AWS calls. Waiting for all background processes to complete..." "INFO"
                wait
                log_message "All background processes completed. Resetting counter." "INFO"
                aws_call_counter=0
            fi
            
            log_message "Completed iteration $iteration of $max_iterations for table $TABLE_NAME" "INFO"
            echo "================================================"
            echo "Statistics collected for table: $TABLE_NAME"
            echo "Region: $region"
            echo "Time range:"
            echo "Start: $start_time"
            echo "End:   $current_time"
            echo "Period: 1 second"
            echo "AWS calls made in this session: $aws_call_counter"
            echo "================================================"
            
            # Update times for next iteration (macOS date syntax)
            current_time=$start_time
            start_time=$(date -u -v-20M -j -f "%Y-%m-%dT%H:%M:%SZ" "$start_time" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Increment iteration counter
            iteration=$((iteration + 1))
        done
        
        # Reset iteration counter for next table
        iteration=1
        current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        start_time=$(date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ")
        
        # Consolidate log files for this table
        consolidate_table_logs "$table_name" "$region"
    done
    
    # Final wait to ensure all background processes are complete
    wait
    
    log_message "Metrics collection completed successfully for all tables" "INFO"
    echo "================================================"
    echo "Completed collection of statistics for all tables"
    echo "Total iterations per table: $max_iterations"
    echo "Time period: 1 second"
    echo "Total AWS calls made: $aws_call_counter"
    echo "================================================"
}

# Execute main function
log_message "About to execute main function..." "DEBUG"
main "$@" 