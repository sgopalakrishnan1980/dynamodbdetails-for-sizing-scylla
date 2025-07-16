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
TABLE_NAMES=()  # Array to store table names to process
USE_INSTANCE_PROFILE=false
TOTAL_AWS_CALLS=0  # Global counter for all AWS calls made during script execution
WAIT_AWS_CALLS=0   # Counter for AWS calls since last wait, used to determine when to wait
# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Initialize log file
LOG_FILE="${LOG_DIR}/script_execution_$(date +%Y%m%d_%H%M%S).log"

# Function to log messages with timestamp
log_message() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local message="$1"
    local level="${2:-INFO}"  # Default to INFO if level not specified
    
    echo "[$timestamp] [$level] $message" | tee -a "$LOG_FILE"
}

# Log initial counter values
log_message "Initial AWS call counter: $TOTAL_AWS_CALLS" "DEBUG"
log_message "Initial wait AWS call counter: $WAIT_AWS_CALLS" "DEBUG"

# Arrays for operation types
READ_OPERATIONS=("GetItem" "Query" "Scan")
WRITE_OPERATIONS=("PutItem" "UpdateItem" "DeleteItem" "BatchWriteItem")

# Initialize variables
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
START_TIME=$(date -u -v-20M +"%Y-%m-%dT%H:%M:%SZ")
START_TIME_FILENAME=$(echo "$START_TIME" | sed 's/:/_/g' | sed 's/T/_/g')
END_TIME_FILENAME=$(echo "$CURRENT_TIME" | sed 's/:/_/g' | sed 's/T/_/g')
default_region=$(aws configure get region $profile_arg 2>&1)

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
    echo "Usage: $0 [-t <table_names>] [-p <aws_profile>] [-r <regions>] [-I]"
    echo "Options:"
    echo "  -t <table_names> Optional: Comma-separated list of specific tables to process. If not provided, all tables will be processed."
    echo "  -p <aws_profile> Optional: AWS profile to use"
    echo "  -r <regions>     Optional: Comma-separated list of regions to process. If not provided, uses current region."
    echo "  -I               Optional: Use EC2 Instance Profile for authentication"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Process all tables in current region"
    echo "  $0 -t mytable                         # Process only 'mytable' in current region"
    echo "  $0 -t table1,table2,table3           # Process only 'table1', 'table2', and 'table3' in current region"
    echo "  $0 -r us-east-1                      # Process all tables in us-east-1"
    echo "  $0 -t mytable -r us-east-1,us-west-2 # Process 'mytable' in specified regions"
    echo "  $0 -t table1,table2 -r us-east-1     # Process 'table1' and 'table2' in us-east-1"
    exit 1
}

# Parse command line arguments
while getopts "t:p:r:I" opt; do
    case $opt in
        t) IFS=',' read -ra TABLE_NAMES <<< "$OPTARG";;
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
    if [ ${#TABLE_NAMES[@]} -gt 0 ]; then
        args_str="$args_str -t ${TABLE_NAMES[*]}"
    fi
    if [ ${#REGIONS_TO_PROBE[@]} -gt 0 ]; then
        args_str="$args_str -r ${REGIONS_TO_PROBE[*]}"
    fi
    if [ "$USE_INSTANCE_PROFILE" = true ]; then
        args_str="$args_str -I"
    fi
    
    log_message "Script started with arguments: $args_str" "INFO"
    if [ ${#TABLE_NAMES[@]} -gt 0 ]; then
        log_message "TABLE_NAMES: ${TABLE_NAMES[*]}" "INFO"
    else
        log_message "No specific tables specified, will process all tables" "INFO"
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
    local region=$2
    local start_time=$3
    local current_time=$4
    local iteration=$5
    local period=${6:-$PERIOD}  # Use provided period or default to global PERIOD
    log_function_call "get_sample_counts" "$table_name" "$region" "$start_time" "$current_time" "$iteration" "$period"
    
    # Validate input parameter
    if [ -z "$table_name" ]; then
        log_message "Error: Table name is required for get_sample_counts" "ERROR"
        return 1
    fi
    
    # Create region and table directory if it doesn't exist
    local region_dir="${LOG_DIR}/${region}"
    local table_dir="${region_dir}/${table_name}"
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
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $period --statistics SampleCount --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get metrics and append to log file in background
        # Increment both AWS call counters before starting background process
        TOTAL_AWS_CALLS=$((TOTAL_AWS_CALLS + 1))
        WAIT_AWS_CALLS=$((WAIT_AWS_CALLS + 1))
        log_message "AWS call counters incremented - Total: $TOTAL_AWS_CALLS, Wait: $WAIT_AWS_CALLS" "DEBUG"
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $period \
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
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $period --statistics SampleCount --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get metrics and append to log file in background
        # Increment both AWS call counters before starting background process
        TOTAL_AWS_CALLS=$((TOTAL_AWS_CALLS + 1))
        WAIT_AWS_CALLS=$((WAIT_AWS_CALLS + 1))
        log_message "AWS call counters incremented - Total: $TOTAL_AWS_CALLS, Wait: $WAIT_AWS_CALLS" "DEBUG"
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $period \
                --statistics SampleCount \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op"  \
                --output text > "$log_file"
            echo "" >> "$log_file"
        ) &
    done
    
    log_message "Sample counts collection completed for iteration $iteration" "INFO"
}

# Function to get P99 latency for all operations
get_p99_latency() {
    local table_name=$1
    local region=$2
    local start_time=$3
    local current_time=$4
    local iteration=$5
    local period=${6:-$PERIOD}  # Use provided period or default to global PERIOD
    log_function_call "get_p99_latency" "$table_name" "$region" "$start_time" "$current_time" "$iteration" "$period"
    
    # Validate input parameter
    if [ -z "$table_name" ]; then
        log_message "Error: Table name is required for get_p99_latency" "ERROR"
        return 1
    fi
    
    # Create region and table directory if it doesn't exist
    local region_dir="${LOG_DIR}/${region}"
    local table_dir="${region_dir}/${table_name}"
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
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $period --extended-statistics p99 --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get P99 metrics and write to log file in background
        # Increment both AWS call counters before starting background process
        TOTAL_AWS_CALLS=$((TOTAL_AWS_CALLS + 1))
        WAIT_AWS_CALLS=$((WAIT_AWS_CALLS + 1))
        log_message "AWS call counters incremented - Total: $TOTAL_AWS_CALLS, Wait: $WAIT_AWS_CALLS" "DEBUG"
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $period \
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
        local aws_cmd="aws cloudwatch get-metric-statistics $profile_arg --namespace AWS/DynamoDB --metric-name SuccessfulRequestLatency --start-time \"$start_time\" --end-time \"$current_time\" --period $period --extended-statistics p99 --dimensions Name=TableName,Value=\"$table_name\" Name=Operation,Value=\"$op\" --output text"
        
        # Log the AWS CLI command
        log_message "Executing CloudWatch API call:" "INFO"
        echo "Executing CloudWatch API call:" | tee -a "$LOG_FILE"
        log_message "$aws_cmd" "INFO"
        echo "$aws_cmd" | tee -a "$LOG_FILE"
        
        # Get P99 metrics and write to log file in background
        # Increment both AWS call counters before starting background process
        TOTAL_AWS_CALLS=$((TOTAL_AWS_CALLS + 1))
        WAIT_AWS_CALLS=$((WAIT_AWS_CALLS + 1))
        log_message "AWS call counters incremented - Total: $TOTAL_AWS_CALLS, Wait: $WAIT_AWS_CALLS" "DEBUG"
        (
            aws cloudwatch get-metric-statistics $profile_arg \
                --namespace AWS/DynamoDB \
                --metric-name SuccessfulRequestLatency \
                --start-time "$start_time" \
                --end-time "$current_time" \
                --period $period \
                --extended-statistics p99 \
                --dimensions Name=TableName,Value="$table_name" Name=Operation,Value="$op" \
                --output text > "$log_file"
        ) &
        log_message "P99 latency metrics for $op will be saved to: $log_file" "INFO"
    done
    
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
    local period_type=$1  # "3hr" or "7day"
    
    log_message "Starting log consolidation for $period_type period..." "INFO"
    
    # Process each region directory
    for region_dir in "${LOG_DIR}"/*/; do
        if [ ! -d "$region_dir" ]; then
            continue
        fi
        local region_name=$(basename "${region_dir%/}")
        log_message "Processing region: $region_name" "INFO"
        # Process each table directory within the region
        for table_dir in "$region_dir"/*/; do
            if [ ! -d "$table_dir" ]; then
                continue
            fi
            local table_name=$(basename "${table_dir%/}")
            log_message "Processing table: $table_name in region: $region_name" "INFO"
            
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
                    local consolidated_sample_count="${table_dir}/${table_name}_${op}_sample_count-${period_type}.log"
                    
                    # Add header to consolidated file
                    {
                        echo "================================================"
                        echo "TABLE: $table_name"
                        echo "OPERATION: $op"
                        echo "METRIC: SampleCount"
                        if [ "$period_type" = "3hr" ]; then
                            echo "PERIOD: 3 hours (20-minute intervals)"
                        else
                            echo "PERIOD: 7 days (24-hour intervals)"
                        fi
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
                        
                        # Delete raw files after consolidation
                        # find "$sample_count_dir" -name "*.log" -type f -delete
                        # log_message "Deleted raw sample count files for $op operation" "INFO"
                    else
                        log_message "No sample count log files found for $op operation" "DEBUG"
                    fi
                fi
                
                # Process p99_latency metrics
                local p99_latency_dir="${op_dir}/p99_latency"
                if [ -d "$p99_latency_dir" ]; then
                    local consolidated_p99_latency="${table_dir}/${table_name}_${op}_p99_latency-${period_type}.log"
                    
                    # Add header to consolidated file
                    {
                        echo "================================================"
                        echo "TABLE: $table_name"
                        echo "OPERATION: $op"
                        echo "METRIC: P99 Latency"
                        if [ "$period_type" = "3hr" ]; then
                            echo "PERIOD: 3 hours (20-minute intervals)"
                        else
                            echo "PERIOD: 7 days (24-hour intervals)"
                        fi
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
                        
                        # Delete raw files after consolidation
                        # find "$p99_latency_dir" -name "*.log" -type f -delete
                        # log_message "Deleted raw P99 latency files for $op operation" "INFO"
                    else
                        log_message "No P99 latency log files found for $op operation" "DEBUG"
                    fi
                fi
            done
        done
    done
    
    log_message "Log consolidation completed for $period_type period" "INFO"
    echo "================================================"
    echo "Log consolidation completed for $period_type period"
    echo "Consolidated files created in: $LOG_DIR"
    echo "Raw files preserved for reference"
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
            
            # If TABLE_NAMES is specified, only process those tables
            if [ ${#TABLE_NAMES[@]} -gt 0 ] && [[ ! " ${TABLE_NAMES[@]} " =~ " $table_name " ]]; then
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
        
        log_message "Starting processing for table: $table_name in region $region" "INFO"
        
        # Set AWS region for this table
        export AWS_DEFAULT_REGION="$region"
        
        while [ $iteration -le $max_iterations ]; do
            log_message "Starting iteration $iteration of $max_iterations for table $table_name" "INFO"
            log_message "Time range: $start_time to $current_time" "INFO"
            
            # Validate environment and variables
            validate_variables
            
            # Get sample counts
            log_message "Collecting sample counts for iteration $iteration..." "INFO"
            if ! get_sample_counts "$table_name" "$region" "$start_time" "$current_time" "$iteration" "1"; then
                log_message "Error: Failed to collect sample counts for iteration $iteration" "ERROR"
                continue
            fi
            
            # Get P99 latency
            log_message "Collecting P99 latency metrics for iteration $iteration..." "INFO"
            if ! get_p99_latency "$table_name" "$region" "$start_time" "$current_time" "$iteration" "1"; then
                log_message "Error: Failed to collect P99 latency metrics for iteration $iteration" "ERROR"
                continue
            fi
            
            # Centralized wait check after processing both sample counts and P99 latency
            if [ $WAIT_AWS_CALLS -ge 1000 ]; then
                log_message "Wait counter reached $WAIT_AWS_CALLS, waiting for background processes..." "INFO"
                wait
                log_message "Background processes completed, resetting counter..." "INFO"
                WAIT_AWS_CALLS=0
            fi
            
            log_message "Completed iteration $iteration of $max_iterations for table $table_name" "INFO"
            echo "================================================"
            echo "Statistics collected for table: $table_name"
            echo "Region: $region"
            echo "Time range:"
            echo "Start: $start_time"
            echo "End:   $current_time"
            echo "Period: 1 second"
            echo "Total AWS calls made: $TOTAL_AWS_CALLS"
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
    done
    
    # Consolidate all 3-hour collection log files
    log_message "Starting consolidation of 3-hour collection data..." "INFO"
    consolidate_table_logs "3hr"
    
    # Continue with 7-day metrics collection (60-second period)
    log_message "Starting DynamoDB metrics collection for last 7 days in 24-hour intervals..." "INFO"
    
    # Initialize iteration counter and AWS call counter for 7-day collection
    local iteration_7d=1
    local max_iterations_7d=7
    
    # Initialize time variables for the first iteration (7 days)
    local current_time_7d=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local start_time_7d=$(date -u -v-24H +"%Y-%m-%dT%H:%M:%SZ")
    
    # Process each table for 7-day collection
    for table_info in "${tables_with_regions[@]}"; do
        # Split table name and region
        IFS=':' read -r table_name region <<< "$table_info"
        
        # Skip if table name is empty
        if [ -z "$table_name" ]; then
            log_message "Skipping empty table name" "WARN"
            continue
        fi
        
        log_message "Starting 7-day processing for table: $table_name in region $region" "INFO"
        
        # Set AWS region for this table
        export AWS_DEFAULT_REGION="$region"
        
        while [ $iteration_7d -le $max_iterations_7d ]; do
            log_message "Starting 7-day iteration $iteration_7d of $max_iterations_7d for table $table_name" "INFO"
            log_message "Time range: $start_time_7d to $current_time_7d" "INFO"
            
            # Validate environment and variables
            validate_variables
            
            # Get sample counts
            log_message "Collecting sample counts for 7-day iteration $iteration_7d..." "INFO"
            if ! get_sample_counts "$table_name" "$region" "$start_time_7d" "$current_time_7d" "$iteration_7d" "60"; then
                log_message "Error: Failed to collect sample counts for 7-day iteration $iteration_7d" "ERROR"
                continue
            fi
            
            # Get P99 latency
            log_message "Collecting P99 latency metrics for 7-day iteration $iteration_7d..." "INFO"
            if ! get_p99_latency "$table_name" "$region" "$start_time_7d" "$current_time_7d" "$iteration_7d" "60"; then
                log_message "Error: Failed to collect P99 latency metrics for 7-day iteration $iteration_7d" "ERROR"
                continue
            fi
            
            # Centralized wait check after processing both sample counts and P99 latency
            if [ $WAIT_AWS_CALLS -ge 1000 ]; then
                log_message "Wait counter reached $WAIT_AWS_CALLS, waiting for background processes..." "INFO"
                wait
                log_message "Background processes completed, resetting counter..." "INFO"
                WAIT_AWS_CALLS=0
            fi
            
            log_message "Completed 7-day iteration $iteration_7d of $max_iterations_7d for table $table_name" "INFO"
            echo "================================================"
            echo "7-Day Statistics collected for table: $table_name"
            echo "Region: $region"
            echo "Time range:"
            echo "Start: $start_time_7d"
            echo "End:   $current_time_7d"
            echo "Period: 60 seconds"
            echo "Total AWS calls made: $TOTAL_AWS_CALLS"
            echo "================================================"
            
            # Update times for next iteration (macOS date syntax)
            current_time_7d=$start_time_7d
            start_time_7d=$(date -u -v-24H -j -f "%Y-%m-%dT%H:%M:%SZ" "$start_time_7d" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Increment iteration counter
            iteration_7d=$((iteration_7d + 1))
        done
        
        # Reset iteration counter for next table
        iteration_7d=1
        current_time_7d=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        start_time_7d=$(date -u -v-24H +"%Y-%m-%dT%H:%M:%SZ")
    done
    
    # Consolidate all 7-day collection log files
    log_message "Starting consolidation of 7-day collection data..." "INFO"
    consolidate_table_logs "7day"
    
    # Final wait to ensure all background processes are complete only if wait AWS call counter is 1000 or more
    if [ $WAIT_AWS_CALLS -ge 1000 ]; then
        log_message "Final wait: Wait AWS call counter reached $WAIT_AWS_CALLS, waiting for all background processes to complete..." "INFO"
        wait
        log_message "All background processes completed, resetting wait counter..." "INFO"
        WAIT_AWS_CALLS=0
        log_message "Final wait AWS call counter reset to: $WAIT_AWS_CALLS" "DEBUG"
    else
        log_message "Final check: Wait AWS call counter is $WAIT_AWS_CALLS (less than 1000), finishing without final wait..." "DEBUG"
    fi
    
    log_message "Metrics collection completed successfully for all tables" "INFO"
    echo "================================================"
    echo "Completed collection of statistics for all tables"
    echo ""
    echo "Collection Summary:"
    echo "  - 3-Hour Collection: $max_iterations iterations per table (1-second period)"
    echo "  - 7-Day Collection: $max_iterations_7d iterations per table (60-second period)"
    echo "  - Total AWS API calls made: $TOTAL_AWS_CALLS"
    echo "================================================"
}

# Execute main function
log_message "About to execute main function..." "DEBUG"
main "$@" 