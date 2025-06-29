#!/bin/bash
#set -x

# ============================================================================
# DynamoDB and ScyllaDB Sizing Collection Script
# This script collects comprehensive sizing and usage metrics for DynamoDB tables
# to help with capacity planning and performance analysis.
# ============================================================================

# Initialize logging
initialize_logging() {
    # Create log file with timestamp
    LOG_FILE="dynamodb_sizing_$(date +%Y%m%d_%H%M%S).log"
    
    # Create log directory if it doesn't exist
    mkdir -p "logs"
    
    # Move log file to logs directory
    LOG_FILE="logs/$LOG_FILE"
    
    # Redirect stdout and stderr to both the log file and the terminal
    exec > >(tee -a "$LOG_FILE") 2>&1
    
    log "INFO" "Logging initialized"
    log "INFO" "Log file: $LOG_FILE"
}

# Log function
log() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message"
}

# Print banner
print_banner() {
    log "INFO" "========================================================"
    log "INFO" "DynamoDB Sizing Script"
    log "INFO" "========================================================"
    log "INFO" "Start time: $(date '+%Y-%m-%d %H:%M:%S')"
    log "INFO" "Log file: $LOG_FILE"
    log "INFO" "========================================================"
    log "INFO" ""
}

# ============================================================================
# Help Section
# ============================================================================
show_help() {
    log "INFO" "========================================================"
    log "INFO" "DynamoDB Sizing Script Help"
    log "INFO" "========================================================"
    log "INFO" "Usage: $0 [options]"
    log "INFO" ""
    log "INFO" "Options:"
    log "INFO" "  -h, --help     Show this help message"
    log "INFO" "  -pre PREFIX    Filter tables by prefix"
    log "INFO" "  -post POSTFIX  Filter tables by postfix"
    log "INFO" "  -both          Use both prefix and postfix filters"
    log "INFO" "  -all           Process all tables (ignore filters)"
    log "INFO" "  -p PROFILE     AWS profile to use"
    log "INFO" "  -a ACCOUNT     AWS account number"
    log "INFO" "  -d DAYS        Number of days to analyze (default: 7)"
    log "INFO" ""
    log "INFO" "Examples:"
    log "INFO" "  $0 -pre dev- -post -prod"
    log "INFO" "  $0 -all -p myprofile -a 123456789012"
    log "INFO" "  $0 -pre test- -d 45"
    log "INFO" "========================================================"
    exit 0
}

# ============================================================================
# Variable Initialization Section
# ============================================================================
initialize_variables() {
    log "INFO" "Initializing variables"
    log "INFO" "Arguments passed to script: $*"
    
    # Log all arguments
    for arg in "$@"; do
        log "INFO" "Argument: $arg"
    done
    
    # Default values
    TABLE_PREFIX="false"
    TABLE_POSTFIX="false"
    USE_BOTH=false
    PROCESS_ALL=false
    AWS_PROFILE="default"
    ACCOUNT_NUMBER=""
    DAYS=7
    
    # Processing counters
    PROCESSED_TABLES=0
    TABLES_SKIPPED=0
    TOTAL_TABLES=0
    PARALLEL_PROCESSES=0
    INITIAL_CLOUDWATCH_CALLS=0
    ESTIMATED_SECONDS=0
    TOTAL_CLOUDWATCH_CALLS=0
    
    # Start time
    START_TIME=$(date +%s)
    
    # Operations to process
    OPERATIONS=("GetItem" "PutItem" "UpdateItem" "DeleteItem" "Query" "Scan")
    
    # Time windows (name,start_time,end_time,period)
    declare -gA TIME_WINDOWS
    local current_time=$(date +%s)
    TIME_WINDOWS["Last 3 hours"]="$((current_time - 10800)),$current_time,1"  # 1 second period for 3 hours
    TIME_WINDOWS["Last 7 days"]="$((current_time - 604800)),$current_time,60"  # 1 minute period for 7 days
    
    # Tables to exclude
    EXCLUDE_TABLES=("dynamodb-metrics" "dynamodb-samples")
    
    # Set PROCESS_ALL to true if no prefix or postfix is specified
    if [ "$TABLE_PREFIX" = "false" ] && [ "$TABLE_POSTFIX" = "false" ]; then
        PROCESS_ALL=true
        log "INFO" "No prefix or postfix specified, setting PROCESS_ALL to true"
    fi
    
    log "INFO" "Variables initialized:"
    log "INFO" "  - Days to analyze: $DAYS"
    log "INFO" "  - Operations: ${OPERATIONS[*]}"
    log "INFO" "  - Time windows: ${!TIME_WINDOWS[*]}"
    log "INFO" "  - Excluded tables: ${EXCLUDE_TABLES[*]}"
    log "INFO" "  - Table Prefix: $TABLE_PREFIX"
    log "INFO" "  - Table Postfix: $TABLE_POSTFIX"
    log "INFO" "  - Process All: $PROCESS_ALL"
    log "INFO" "  - Use Both: $USE_BOTH"
}

# Get period for time window
get_period_for_window() {
    local seconds=$1
    
    if [ "$seconds" -eq 10800 ]; then  # 3 hours
        echo 1  # 1 second period
    elif [ "$seconds" -eq 604800 ]; then  # 7 days
        echo 60  # 1 minute period
    else  # 15+ days
        echo 300  # 5 minute period
    fi
}

# ============================================================================
# Function Definitions Section
# ============================================================================

# AWS Configuration
setup_aws_config() {
    local profile=$1
    local account=$2
    
    log "INFO" "Setting up AWS configuration"
    log "INFO" "  - Profile: $profile"
    log "INFO" "  - Account: $account"
    
    # Set AWS profile
    if [ -n "$profile" ]; then
        log "INFO" "Using AWS profile: $profile"
        export AWS_PROFILE="$profile"
    else
        log "INFO" "Using default AWS profile"
        export AWS_PROFILE="default"
    fi
    
    # Verify AWS credentials
    if ! aws sts get-caller-identity &>/dev/null; then
        log "ERROR" "Failed to verify AWS credentials"
        return 1
    fi
    
    # Get account number if not provided
    if [ -z "$account" ]; then
        log "INFO" "Retrieving AWS account number"
        account=$(aws sts get-caller-identity --query 'Account' --output text)
        if [ $? -ne 0 ]; then
            log "ERROR" "Failed to retrieve AWS account number"
            return 1
        fi
    fi
    
    # Get region
    local region=$(aws configure get region)
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to get AWS region"
        return 1
    fi
    
    log "INFO" "AWS configuration completed successfully"
    log "INFO" "  - Account: $account"
    log "INFO" "  - Region: $region"
    
    return 0
}

# Setup Output Directory
setup_output_directory() {
    local account_number=$1
    local timestamp=$(date +%Y%m%d_%H%M%S)
    
    # Sanitize account number to ensure safe directory name
    local safe_account=$(echo "$account_number" | tr -cd '[:alnum:]_-')
    OUTPUT_DIR="dynamodb_sizing_${safe_account}_${timestamp}"
    
    log "INFO" "Creating output directory: $OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
    
    # Initialize output files with safe paths
    SUMMARY_FILE="${OUTPUT_DIR}/summary.csv"
    DETAILED_FILE="${OUTPUT_DIR}/detailed.csv"
    
    # Create and initialize output files
    echo "Table,Operation,TimeWindow,P99Latency,AvgLatency,SampleCount" > "$SUMMARY_FILE"
    echo "Table,Operation,TimeWindow,P99Latency,AvgLatency,SampleCount" > "$DETAILED_FILE"
    
    # Verify files were created
    if [ ! -f "$SUMMARY_FILE" ] || [ ! -f "$DETAILED_FILE" ]; then
        log "ERROR" "Failed to initialize output files"
        return 1
    fi
    
    log "INFO" "Output files initialized:"
    log "INFO" "  - Summary file: $SUMMARY_FILE"
    log "INFO" "  - Detailed file: $DETAILED_FILE"
    
    return 0
}

# Retrieve metadata for tables
retrieve_metadata() {
    local region="$1"
    local prefix="$2"
    local postfix="$3"
    local use_both="$4"
    local process_all="$5"

    for arg in "$@"; do
        echo "Argument: $arg"
    done
    
    log "INFO" "Retrieving metadata for region: $region"
    log "INFO" "Parameters:"
    log "INFO" "  - Prefix: $prefix"
    log "INFO" "  - Postfix: $postfix"
    log "INFO" "  - Use both: $use_both"
    log "INFO" "  - Process all: $process_all"
    
    # Get list of tables
    local tables=""
    if [ "$process_all" = true ]; then
        log "INFO" "Retrieving all tables"
        tables=$(aws dynamodb list-tables --region "$region" --query 'TableNames[]' --output text)
        if [ $? -ne 0 ]; then
            log "ERROR" "Failed to list tables"
            return 1
        fi
    else
        if [ "$use_both" = true ]; then
            log "INFO" "Retrieving tables with both prefix and postfix"
            tables=$(aws dynamodb list-tables --region "$region" --query "TableNames[?starts_with(@, '$prefix') && ends_with(@, '$postfix')]" --output text)
            if [ $? -ne 0 ]; then
                log "ERROR" "Failed to list tables with prefix '$prefix' and postfix '$postfix'"
                return 1
            fi
        else
            if [ -n "$prefix" ]; then
                log "INFO" "Retrieving tables with prefix: $prefix"
                tables=$(aws dynamodb list-tables --region "$region" --query "TableNames[?starts_with(@, '$prefix')]" --output text)
                if [ $? -ne 0 ]; then
                    log "ERROR" "Failed to list tables with prefix '$prefix'"
                    return 1
                fi
            elif [ -n "$postfix" ]; then
                log "INFO" "Retrieving tables with postfix: $postfix"
                tables=$(aws dynamodb list-tables --query "TableNames[?ends_with(@, '$postfix')]" --output text)
                if [ $? -ne 0 ]; then
                    log "ERROR" "Failed to list tables with postfix '$postfix'"
                    return 1
                fi
            else
                log "ERROR" "No prefix or postfix specified"
                return 1
            fi
        fi
    fi
    
    if [ -z "$tables" ]; then
        log "ERROR" "No tables found matching criteria"
        return 1
    fi
    
    # Count total tables
    local total_tables=$(echo "$tables" | wc -l)
    log "INFO" "Found $total_tables tables"
    
    # Calculate parallel processes (max 50)
    local parallel_processes=$((total_tables > 50 ? 50 : total_tables))
    log "INFO" "Using $parallel_processes parallel processes"
    
    # Calculate initial CloudWatch calls
    local initial_cloudwatch_calls=$((total_tables * 2))  # 2 calls per table (P99 and sample count)
    log "INFO" "Estimated initial CloudWatch calls: $initial_cloudwatch_calls"
    
    # Calculate estimated time (2 seconds per table)
    local estimated_seconds=$((total_tables * 2))
    log "INFO" "Estimated processing time: $estimated_seconds seconds"
    
    # Return metadata
    echo "${total_tables}:${parallel_processes}:${initial_cloudwatch_calls}:${estimated_seconds}:${tables}"
    return 0
}

# Parallel Process Calculation
calculate_parallel_processes() {
    local num_tables=$1
    local max_even=6
    local max_odd=7
    
    if [ "$num_tables" -le 2 ]; then
        echo 1
        return
    fi
    
    if [ $((num_tables % 2)) -eq 0 ]; then
        if [ "$num_tables" -le "$max_even" ]; then
            echo "$num_tables"
        else
            echo "$max_even"
        fi
    else
        if [ "$num_tables" -le "$max_odd" ]; then
            echo "$num_tables"
        else
            echo "$max_odd"
        fi
    fi
}

# CloudWatch Call Management
increment_cloudwatch_calls() {
    local calls=$1
    local lock_file="$OUTPUT_DIR/cloudwatch_calls.lock"
    (
        flock -x 200
        TOTAL_CLOUDWATCH_CALLS=$((TOTAL_CLOUDWATCH_CALLS + calls))
    ) 200>"$lock_file"
}

# Get CloudWatch calls count
get_cloudwatch_calls() {
    echo "$TOTAL_CLOUDWATCH_CALLS"
}

# CloudWatch API Call Function
make_cloudwatch_call() {
    local operation="$1"
    local start_time="$2"
    local end_time="$3"
    local period="$4"
    local output_file="$5"
    local table_name="$6"
    
    echo "      - Making CloudWatch call for $operation"
    echo "        Time range: $start_time to $end_time"
    
    # Initialize variables
    local call_counter=0
    local background_jobs=()
    local max_parallel_jobs=8  # For 3-hour window
    
    # Check if this is a 3-hour window (period = 1)
    if [ "$period" = "1" ]; then
        log "INFO" "Processing 3-hour window with $max_parallel_jobs parallel jobs"
        local current_end_time="$end_time"
        local chunk_size=1200  # 20 minutes in seconds
        
        # Process in 8 parallel chunks of 20 minutes each
        for i in {1..8}; do
            # Calculate window times
            local current_end_epoch=$(date -u -d "$current_end_time" +%s)
            local window_start_epoch=$((current_end_epoch - chunk_size))
            local window_start=$(date -u -d "@$window_start_epoch" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Wait if we've reached max parallel jobs
            while [ ${#background_jobs[@]} -ge $max_parallel_jobs ]; do
                wait -n
                # Remove completed jobs from array
                background_jobs=($(jobs -p))
            done
            
            # Make the CloudWatch call in background
            make_sample_count_call "$operation" "$window_start" "$current_end_time" "$period" "$output_file" "$table_name" &
            background_jobs+=($!)
            call_counter=$((call_counter + 1))
            
            # Update for next iteration
            current_end_time="$window_start"
        done
        
        # Wait for all jobs to complete
        wait
        
    else
        # For 7-day window, process one day at a time
        log "INFO" "Processing 7-day window sequentially"
        local current_end_time="$end_time"
        local chunk_size=86400  # 1 day in seconds
        
        # Process 7 days sequentially
        for i in {1..7}; do
            # Calculate window times
            local current_end_epoch=$(date -u -d "$current_end_time" +%s)
            local window_start_epoch=$((current_end_epoch - chunk_size))
            local window_start=$(date -u -d "@$window_start_epoch" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Make the CloudWatch call
            make_sample_count_call "$operation" "$window_start" "$current_end_time" "$period" "$output_file" "$table_name"
            call_counter=$((call_counter + 1))
            
            # Update for next iteration
            current_end_time="$window_start"
        done
    fi
    
    echo "    Completed $call_counter sample count calls for $operation"
    return 0
}

# Helper function to make sample count CloudWatch calls
make_sample_count_call() {
    local operation="$1"
    local start_time="$2"
    local end_time="$3"
    local period="$4"
    local output_file="$5"
    local table_name="$6"
    
    # Convert epoch timestamps to ISO format for CloudWatch
    local start_time_iso=$(date -u -d "$start_time" +"%Y-%m-%dT%H:%M:%SZ")
    local end_time_iso=$(date -u -d "$end_time" +"%Y-%m-%dT%H:%M:%SZ")
    
    # Sanitize operation name for filename
    local safe_operation=$(echo "$operation" | tr -cd '[:alnum:]_-')
    
    # Format timestamps for filename (remove Z and replace special characters)
    local start_time_filename=$(echo "$start_time_iso" | sed 's/Z$//' | sed 's/:/_/g' | sed 's/T/_/g')
    local end_time_filename=$(echo "$end_time_iso" | sed 's/Z$//' | sed 's/:/_/g' | sed 's/T/_/g')
    
    # Create output filename with safe characters
    local output_filename="${table_name}_${safe_operation}Latency_${start_time_filename}_to_${end_time_filename}.json"
    local output_path="${output_file}/${output_filename}"
    
    # Ensure output directory exists
    mkdir -p "$output_file"
    
    # Log the complete CloudWatch API call
    log "INFO" "Making Sample Count CloudWatch API call:"
    log "INFO" "  aws cloudwatch get-metric-statistics \\"
    log "INFO" "    --namespace AWS/DynamoDB \\"
    log "INFO" "    --metric-name ${operation}Latency \\"
    log "INFO" "    --start-time \"$start_time_iso\" \\"
    log "INFO" "    --end-time \"$end_time_iso\" \\"
    log "INFO" "    --period \"$period\" \\"
    log "INFO" "    --statistics SampleCount \\"
    log "INFO" "    --dimensions Name=TableName,Value=\"$table_name\" \\"
    log "INFO" "    --region \"$REGION\" \\"
    log "INFO" "    --output json"
    
    # Make the CloudWatch API call
    local cloudwatch_output
    touch "$output_path"
   aws cloudwatch get-metric-statistics \
        --namespace AWS/DynamoDB \
        --metric-name ${operation}Latency \
        --start-time "$start_time_iso" \
        --end-time "$end_time_iso" \
        --period "$period" \
        --statistics SampleCount \
        --dimensions Name=TableName,Value="$table_name" \
        --region "$REGION" \
        --output json >> $output_path
    
    # Log the response
    log "INFO" "CloudWatch API Response:"
   # log "INFO" "$cloudwatch_output"
    
    # Check for AWS CLI errors
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to make CloudWatch call for $operation sample count: $cloudwatch_output"
        rm -f "$output_path"
        return 1
    fi
    
    # Save the raw output
 #   echo "$cloudwatch_output" > "$output_path"
    
    # Check if the file was created and has content
    if [ ! -s "$output_path" ]; then
        log "ERROR" "Empty output file created for $operation sample count"
        rm -f "$output_path"
        return 1
    fi
    
    # Process the output
    if ! jq -e '.' "$output_path" > /dev/null 2>&1; then
        log "ERROR" "Invalid JSON in CloudWatch response"
        rm -f "$output_path"
        return 1
    fi
    
    if ! jq --arg ts "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" '
        if .Datapoints then
            . + {
                "Timestamp": $ts,
                "SamplePoints": [.Datapoints[] | {
                    "Timestamp": .Timestamp,
                    "SampleCount": (.SampleCount // 0)
                }]
            }
        else
            {
                "Timestamp": $ts,
                "SamplePoints": [],
                "Datapoints": []
            }
        end' "$output_path" > "${output_path}.tmp"; then
        log "ERROR" "Failed to process sample count data for $operation"
        rm -f "$output_path" "${output_path}.tmp"
        return 1
    fi
    
    mv "${output_path}.tmp" "$output_path"
    
    # Extract and log sample count
    local sample_count
    if ! sample_count=$(jq -r 'if .Datapoints and (.Datapoints | length) > 0 then .Datapoints[0].SampleCount // 0 else 0 end' "$output_path"); then
        log "ERROR" "Failed to extract sample count from $output_path"
        sample_count=0
    fi
    
    if [ "$sample_count" = "null" ]; then
        sample_count=0
    fi
    
    log "INFO" "Sample count for ${start_time_filename} to ${end_time_filename}: $sample_count"
    
    # Increment CloudWatch calls counter
    increment_cloudwatch_calls 1
    
    return 0
}

# P99 Latency Calculation
get_p99_latency() {
    local table_name="$1"
    local region="$2"
    local operation="$3"
    local start_time="$4"
    local end_time="$5"
    local output_dir="$6"
    local period="$7"
    
    # Validate inputs
    if [ -z "$table_name" ]; then
        log "ERROR" "Table name is empty"
        return 1
    fi
    if [ -z "$period" ]; then
        log "ERROR" "Period argument is empty for $operation"
        return 1
    fi
    
    for arg in "$@"; do
        echo "Argument: $arg"   
    done
    echo "    - Getting P99 latency for $operation from $start_time to $end_time"
    echo "    - Using period: $period"
    echo "    - Table name: $table_name"
    
    # Initialize variables
    local call_counter=0
    local background_jobs=()
    local max_parallel_jobs=8  # For 3-hour window
    
    # Check if this is a 3-hour window (period = 1)
    if [ "$period" = "1" ]; then
        log "INFO" "Processing 3-hour window with $max_parallel_jobs parallel jobs"
        local current_end_time="$end_time"
        local chunk_size=1200  # 20 minutes in seconds
        
        # Process in 8 parallel chunks of 20 minutes each
        for i in {1..8}; do
            # Calculate window times
            local current_end_epoch=$(date -u -d "$current_end_time" +%s)
            local window_start_epoch=$((current_end_epoch - chunk_size))
            local window_start=$(date -u -d "@$window_start_epoch" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Wait if we've reached max parallel jobs
            while [ ${#background_jobs[@]} -ge $max_parallel_jobs ]; do
                wait -n
                # Remove completed jobs from array
                background_jobs=($(jobs -p))
            done
            
            # Make the CloudWatch call in background
            make_p99_call "$table_name" "$region" "$operation" "$window_start" "$current_end_time" "$output_dir" "$period" &
            background_jobs+=($!)
            call_counter=$((call_counter + 1))
            
            # Update for next iteration
            current_end_time="$window_start"
        done
        
        # Wait for all jobs to complete
        wait
        
    else
        # For 7-day window, process one day at a time
        log "INFO" "Processing 7-day window sequentially"
        local current_end_time="$end_time"
        local chunk_size=86400  # 1 day in seconds
        
        # Process 7 days sequentially
        for i in {1..7}; do
            # Calculate window times
            local current_end_epoch=$(date -u -d "$current_end_time" +%s)
            local window_start_epoch=$((current_end_epoch - chunk_size))
            local window_start=$(date -u -d "@$window_start_epoch" +"%Y-%m-%dT%H:%M:%SZ")
            
            # Make the CloudWatch call
            make_p99_call "$table_name" "$region" "$operation" "$window_start" "$current_end_time" "$output_dir" "$period"
            call_counter=$((call_counter + 1))
            
            # Update for next iteration
            current_end_time="$window_start"
        done
    fi
    
    echo "    Completed $call_counter P99 latency calls for $table_name $operation"
    return 0
}

# Helper function to make P99 CloudWatch calls
make_p99_call() {
    local table_name="$1"
    local region="$2"
    local operation="$3"
    local start_time="$4"
    local end_time="$5"
    local output_dir="$6"
    local period="$7"
    
    # Validate inputs
    if [ -z "$table_name" ]; then
        log "ERROR" "Table name is empty"
        return 1
    fi
    if [ -z "$period" ]; then
        log "ERROR" "Period argument is empty for $operation"
        return 1
    fi
    
    # Convert epoch timestamps to ISO format for CloudWatch
    local start_time_iso=$(date -u -d "$start_time" +"%Y-%m-%dT%H:%M:%SZ")
    local end_time_iso=$(date -u -d "$end_time" +"%Y-%m-%dT%H:%M:%SZ")
    
    # Sanitize table name and operation for filename
    local safe_table=$(echo "$table_name" | tr -cd '[:alnum:]_-')
    local safe_operation=$(echo "$operation" | tr -cd '[:alnum:]_-')
    
    # Format timestamps for filename (remove Z and replace special characters)
    local start_time_filename=$(echo "$start_time_iso" | sed 's/Z$//' | sed 's/:/_/g' | sed 's/T/_/g')
    local end_time_filename=$(echo "$end_time_iso" | sed 's/Z$//' | sed 's/:/_/g' | sed 's/T/_/g')
    
    # Create output filename with safe characters
    local output_filename="${safe_table}_${safe_operation}P99Latency_${start_time_filename}_to_${end_time_filename}.json"
    local output_path="${output_dir}/${output_filename}"
    
    # Ensure output directory exists
    mkdir -p "$output_dir"
    
    # Log the complete CloudWatch API call
    log "INFO" "Making P99 CloudWatch API call:"
    log "INFO" "  aws cloudwatch get-metric-statistics \\"
    log "INFO" "    --namespace AWS/DynamoDB \\"
    log "INFO" "    --metric-name ${operation}Latency \\"
    log "INFO" "    --start-time \"$start_time_iso\" \\"
    log "INFO" "    --end-time \"$end_time_iso\" \\"
    log "INFO" "    --period \"$period\" \\"
    log "INFO" "    --statistics Maximum \\"
    log "INFO" "    --dimensions Name=TableName,Value=\"$table_name\" \\"
    log "INFO" "    --region \"$region\" \\"
    log "INFO" "    --output json"
    
    # Make the CloudWatch API call
    local cloudwatch_output
    touch "$output_path"
    aws cloudwatch get-metric-statistics \
        --namespace AWS/DynamoDB \
        --metric-name ${operation}Latency \
        --start-time "$start_time_iso" \
        --end-time "$end_time_iso" \
        --period "$period" \
        --statistics Maximum \
        --dimensions Name=TableName,Value="$table_name" \
        --region "$region" \
        --output json >> $output_path
    
    # Log the response
  #  log "INFO" "CloudWatch API Response:"
    log "INFO" "$cloudwatch_output"
    
    # Check for AWS CLI errors
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to make CloudWatch call for $operation P99: $cloudwatch_output"
        rm -f "$output_path"
        return 1
    fi
    
    # Save the raw output
#    echo "$cloudwatch_output" > "$output_path"
    
    # Check if the file was created and has content
    if [ ! -s "$output_path" ]; then
        log "ERROR" "Empty output file created for $operation P99"
        rm -f "$output_path"
        return 1
    fi
    
    # Increment CloudWatch calls counter
    increment_cloudwatch_calls 1
    
    return 0
}

# Status Updates
show_status_update() {
    local current_time=$(date +%s)
    local elapsed_time=$((current_time - START_TIME))
    local hours=$((elapsed_time / 3600))
    local minutes=$(((elapsed_time % 3600) / 60))
    local seconds=$((elapsed_time % 60))
    local current_cloudwatch_calls=$(get_cloudwatch_calls)
    
    log "STATUS" "========================================================"
    log "STATUS" "Status Update at $(date '+%Y-%m-%d %H:%M:%S')"
    log "STATUS" "--------------------------------------------------------"
    log "STATUS" "Progress:"
    log "STATUS" "  - Tables processed: $((PROCESSED_TABLES + 1)) of $TOTAL_TABLES"
    log "STATUS" "  - Tables skipped: $TABLES_SKIPPED"
    log "STATUS" "  - Remaining tables: $((TOTAL_TABLES - PROCESSED_TABLES - 1))"
    log "STATUS" "  - Current table: $TABLE"
    log "STATUS" "  - Current operation: $op"
    log "STATUS" "  - Current time window: $name"
    log "STATUS" "--------------------------------------------------------"
    log "STATUS" "Performance:"
    log "STATUS" "  - Elapsed time: ${hours}h ${minutes}m ${seconds}s"
    log "STATUS" "  - CloudWatch calls made: $current_cloudwatch_calls"
    log "STATUS" "  - Average calls per table: $(echo "scale=2; $current_cloudwatch_calls / ($PROCESSED_TABLES + 1)" | bc)"
    log "STATUS" "  - Processing rate: $(echo "scale=2; $PROCESSED_TABLES / ($elapsed_time / 60)" | bc) tables/minute"
    log "STATUS" "========================================================"
}

# Main processing function
process_table() {
    local table=$1
    local op=$2
    local name=$3
    local start_time=$4
    local end_time=$5
    local period=$6

    # Validate inputs
    if [ -z "$table" ]; then
        log "ERROR" "Table name is empty"
        return 1
    fi
    if [ -z "$period" ]; then
        log "ERROR" "Period argument is empty for table $table, operation $op"
        return 1
    fi

    log "INFO" "Processing table: $table, operation: $op, time window: $name"
    log "INFO" "Using region: $REGION"
    log "INFO" "Time range: $start_time to $end_time"
    log "INFO" "Period: $period"
    
    # Get P99 latency
    local p99_latency=$(get_p99_latency "$table" "$REGION" "$op" "$start_time" "$end_time" "$OUTPUT_DIR" "$period")
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to get P99 latency for table $table, operation $op"
        return 1
    fi
    
    # Get sample count
    local sample_count=$(make_sample_count_call "$op" "$start_time" "$end_time" "$period" "$OUTPUT_DIR" "$table")
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to get sample count for table $table, operation $op"
        return 1
    fi
    
    # Calculate average latency
    local avg_latency=$(echo "scale=2; $p99_latency / 2" | bc)
    
    # Write to detailed file
    echo "$table,$op,$name,$p99_latency,$avg_latency,$sample_count" >> "$DETAILED_FILE"
    
    log "INFO" "Completed processing table: $table, operation: $op, time window: $name"
    return 0
}

# Process Tables
process_tables() {
    local region=$1
    local tables=$2
    
    log "INFO" "Starting to process tables in region: $region"
    
    # Export region for use in other functions
    export REGION="$region"
    
    # Process each table
    for table in $tables; do
        PROCESSED_TABLES=$((PROCESSED_TABLES + 1))
        
        # Skip if table is in exclude list
        if [[ " ${EXCLUDE_TABLES[@]} " =~ " ${table} " ]]; then
            log "INFO" "Skipping excluded table: $table"
            TABLES_SKIPPED=$((TABLES_SKIPPED + 1))
            continue
        fi
        
        log "INFO" "Processing table: $table"
        
        # Process each operation and time window
        for op in "${OPERATIONS[@]}"; do
            for name in "${!TIME_WINDOWS[@]}"; do
                IFS=',' read -r start_time end_time period <<< "${TIME_WINDOWS[$name]}"
                
                # Calculate period based on time window
                if [ "$name" = "Last 3 hours" ]; then
                    period=8  # 1 second period for 3 hours
                elif [ "$name" = "Last 7 days" ]; then
                    period=420  # 1 minute period for 7 days
                else
                    period=900  # 5 minute period for longer windows
                fi
                
                # Convert epoch timestamps to UTC format with Z suffix
                local start_time_utc=$(date -u -d "@$start_time" +"%Y-%m-%dT%H:%M:%SZ")
                local end_time_utc=$(date -u -d "@$end_time" +"%Y-%m-%dT%H:%M:%SZ")
                
                log "INFO" "Processing time window: $name"
                log "INFO" "  Start time (UTC): $start_time_utc"
                log "INFO" "  End time (UTC): $end_time_utc"
                log "INFO" "  Period: $period"
                
                # Show status update every 5 tables
                if [ $((PROCESSED_TABLES % 5)) -eq 0 ]; then
                    show_status_update
                fi
                
                process_table "$table" "$op" "$name" "$start_time_utc" "$end_time_utc" "$period"
            done
        done
        
        log "INFO" "Completed processing table: $table"
    done
    
    log "INFO" "Completed processing all tables in region: $region"
}

# DynamoDB Metrics Collection
get_dynamodb_metrics() {
    local TABLE_NAME="$1"
    local REGION="$2"
    local local_cloudwatch_calls=0
    local process_start_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local timeout=3600  # 1 hour timeout per table
    
    log "INFO" "Starting metrics collection for table: $TABLE_NAME"
    log "INFO" "Region: $REGION"
    
    # Create table-specific output directory
    local table_output_dir="$OUTPUT_DIR/$TABLE_NAME"
    mkdir -p "$table_output_dir"
    
    log "INFO" "Step 1: Getting table description"
    local table_info
    table_info=$(aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION" 2>&1)
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to get table description for $TABLE_NAME: $table_info"
        return 1
    fi
    
    # Validate JSON response
    if ! echo "$table_info" | jq -e '.' > /dev/null 2>&1; then
        log "ERROR" "Invalid JSON in table description response"
        return 1
    fi
    
    local creation_date
    if ! creation_date=$(echo "$table_info" | jq -r '.Table.CreationDateTime // empty'); then
        log "ERROR" "Failed to extract creation date from table info"
        return 1
    fi
    
    if [ -z "$creation_date" ]; then
        log "ERROR" "Creation date is empty in table info"
        return 1
    fi
    
    # Convert creation date to UTC format with Z suffix
    local creation_date_utc=$(date -u -d "$creation_date" +"%Y-%m-%dT%H:%M:%SZ")
    log "INFO" "Table creation date: $creation_date_utc"
    
    log "INFO" "Step 2: Setting up output files"
    local summary_file="$table_output_dir/${TABLE_NAME}_summary.json"
    local detailed_file="$table_output_dir/${TABLE_NAME}_detailed.json"
    
    log "INFO" "Output files:"
    log "INFO" "  - Summary: $summary_file"
    log "INFO" "  - Detailed: $detailed_file"
    
    log "INFO" "Step 3: Initializing metrics collection"
    # Start with basic table information
    echo "{
        \"TableName\": \"$TABLE_NAME\",
        \"Region\": \"$REGION\",
        \"CreationDate\": \"$creation_date_utc\",
        \"Metrics\": {
            \"ReadOperations\": {},
            \"WriteOperations\": {}
        }
    }" > "$detailed_file"
    
    log "INFO" "Step 4: Processing operations"
    # Operations to monitor
    local READ_OPS=("GetItem" "BatchGetItem" "Scan")
    local WRITE_OPS=("PutItem" "BatchWriteItem")
    
    log "INFO" "Operations to process:"
    log "INFO" "  - Read: ${READ_OPS[*]}"
    log "INFO" "  - Write: ${WRITE_OPS[*]}"
    
    # Initialize P99 latency variables for each operation
    declare -A OPERATION_P99_LATENCIES
    for op in "${READ_OPS[@]}" "${WRITE_OPS[@]}"; do
        OPERATION_P99_LATENCIES[$op]=0
    done
    
    log "INFO" "Step 5: Collecting P99 latencies"
    for op in "${READ_OPS[@]}" "${WRITE_OPS[@]}"; do
        log "INFO" "Getting P99 latency for $op"
        
        # Get current time in UTC with Z suffix
        local end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        # Calculate start time (1 month ago) in UTC with Z suffix
        local start_time=$(date -u -d "1 month ago" +"%Y-%m-%dT%H:%M:%SZ")
        
        get_p99_latency "$TABLE_NAME" "$REGION" "$op" "$start_time" "$end_time" "$table_output_dir" "$period"
        local_cloudwatch_calls=$((local_cloudwatch_calls + $?))
        
        # Find the maximum P99 value from all time windows
        local max_p99=0
        local found_files=false
        
        for file in "$table_output_dir/${TABLE_NAME}_${op}P99Latency_"*.json; do
            if [ -f "$file" ]; then
                found_files=true
                # Move files from temp to output directory
                local filename=$(basename "$file")
                mv "$file" "$OUTPUT_DIR/$filename"
            fi
        done
        
        if [ "$found_files" = false ]; then
            log "WARN" "No P99 latency files found for operation $op"
            continue
        fi
        
        OPERATION_P99_LATENCIES[$op]=$max_p99
        log "INFO" "Maximum P99 latency for $op: $max_p99"
    done
    
    log "INFO" "Step 6: Processing time windows"
    for window in "${TIME_WINDOWS[@]}"; do
        # Split the window string into seconds and name
        IFS=":" read -r seconds name <<< "$window"
        
        log "INFO" "Processing window: $name ($seconds seconds)"
        
        # Get appropriate period for this time window
        local period=$(get_period_for_window $seconds)
        log "INFO" "Period: $period seconds"
        
        # Calculate time range in UTC with Z suffix
        local end_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        local start_time=$(date -u -d "$seconds seconds ago" +"%Y-%m-%dT%H:%M:%SZ")
        log "INFO" "Time range: $start_time to $end_time"
        
        # Create a temporary directory for parallel processing
        local temp_dir
        temp_dir=$(mktemp -d)
        trap 'rm -rf "$temp_dir"' EXIT
        
        # Process read operations
        for op in "${READ_OPS[@]}"; do
            log "INFO" "Processing $op operations"
            
            # Initialize variables for the time window
            local x=0
            local call_counter=0
            local total_calls=0
            
            # Calculate total expected calls based on period
            if [ "$period" -eq 1 ]; then  # 1 second period
                total_calls=$((seconds / 1))  # One call per second
            elif [ "$period" -eq 60 ]; then  # 1 minute period
                total_calls=$((seconds / 60))  # One call per minute
            else  # 5 minute period
                total_calls=$((seconds / 300))  # One call per 5 minutes
            fi
            
            log "INFO" "Expected calls: $total_calls"
            
            # Process in batches
            while [ $x -lt $seconds ]; do
                # Check for timeout
                local current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
                local elapsed_seconds=$(( $(date -u +%s) - $(date -u -d "$process_start_time" +%s) ))
                if [ $elapsed_seconds -gt $timeout ]; then
                    log "ERROR" "Processing timeout for table $TABLE_NAME after 1 hour"
                    rm -rf "$temp_dir"
                    return 1
                fi
                
                # Calculate window times in UTC with Z suffix
                local window_end=$(date -u -d "$x seconds ago" +"%Y-%m-%dT%H:%M:%SZ")
                local window_start=$(date -u -d "$((x + period)) seconds ago" +"%Y-%m-%dT%H:%M:%SZ")
                
                log "INFO" "Making CloudWatch call #$call_counter"
                log "INFO" "Window: $window_start to $window_end"
                
                # Make the CloudWatch call
                make_cloudwatch_call "$op" "$window_start" "$window_end" "$period" "$temp_dir/output" "$table_name"
                call_counter=$((call_counter + 1))
                
                # Show progress
                log "INFO" "Progress: $call_counter/$total_calls calls completed"
                
                x=$((x + period))
            done
        done
        
        # Clean up
        rm -rf "$temp_dir"
    done
    
    log "INFO" "Step 7: Creating summary file"
    echo "{
        \"TableName\": \"$TABLE_NAME\",
        \"Region\": \"$REGION\",
        \"TotalCloudWatchCalls\": $local_cloudwatch_calls,
        \"P99Latencies\": {
            \"ReadOperations\": {
                \"GetItem\": ${OPERATION_P99_LATENCIES[GetItem]},
                \"BatchGetItem\": ${OPERATION_P99_LATENCIES[BatchGetItem]},
                \"Scan\": ${OPERATION_P99_LATENCIES[Scan]}
            },
            \"WriteOperations\": {
                \"PutItem\": ${OPERATION_P99_LATENCIES[PutItem]},
                \"BatchWriteItem\": ${OPERATION_P99_LATENCIES[BatchWriteItem]}
            }
        }
    }" > "$summary_file"
    
    log "INFO" "Completed processing table $TABLE_NAME"
    log "INFO" "Generated files:"
    log "INFO" "  - $summary_file"
    log "INFO" "  - $detailed_file"
    
    return 0
}

# ============================================================================
# Final Summary Function
# ============================================================================
show_final_summary() {
    local end_time=$(date +%s)
    local total_time=$((end_time - START_TIME))
    local hours=$((total_time / 3600))
    local minutes=$(((total_time % 3600) / 60))
    local seconds=$((total_time % 60))
    local total_cloudwatch_calls=$(get_cloudwatch_calls)
    
    log "SUMMARY" "========================================================"
    log "SUMMARY" "Final Summary"
    log "SUMMARY" "--------------------------------------------------------"
    log "SUMMARY" "Processing Statistics:"
    log "SUMMARY" "  - Total tables processed: $PROCESSED_TABLES"
    log "SUMMARY" "  - Tables skipped: $TABLES_SKIPPED"
    log "SUMMARY" "  - Total CloudWatch calls: $total_cloudwatch_calls"
    log "SUMMARY" "  - Average calls per table: $(echo "scale=2; $total_cloudwatch_calls / $PROCESSED_TABLES" | bc)"
    log "SUMMARY" "--------------------------------------------------------"
    log "SUMMARY" "Time Statistics:"
    log "SUMMARY" "  - Total processing time: ${hours}h ${minutes}m ${seconds}s"
    log "SUMMARY" "  - Average time per table: $(echo "scale=2; $total_time / $PROCESSED_TABLES" | bc) seconds"
    log "SUMMARY" "--------------------------------------------------------"
    log "SUMMARY" "Output Location:"
    log "SUMMARY" "  - Output directory: $OUTPUT_DIR"
    log "SUMMARY" "  - Summary file: $SUMMARY_FILE"
    log "SUMMARY" "  - Detailed file: $DETAILED_FILE"
    log "SUMMARY" "  - Log file: $LOG_FILE"
    log "SUMMARY" "========================================================"
}

# ============================================================================
# Main Function
# ============================================================================
main() {
    # Initialize logging
    initialize_logging
    
    # Print banner
    print_banner
    
    # Check for help flag
    if [ "$#" -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
        show_help
    fi
    
    # Initialize variables
    initialize_variables
    
    # Parse command line arguments
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -pre)
                TABLE_PREFIX="$2"
                shift 2
                ;;
            -post)
                TABLE_POSTFIX="$2"
                shift 2
                ;;
            -both)
                USE_BOTH=true
                shift
                ;;
            -all)
                PROCESS_ALL=true
                shift
                ;;
            -p)
                AWS_PROFILE="$2"
                shift 2
                ;;
            -a)
                ACCOUNT_NUMBER="$2"
                shift 2
                ;;
            -d)
                DAYS="$2"
                shift 2
                ;;
            *)
                log "ERROR" "Unknown option: $1"
                show_help
                ;;
        esac
    done
    
    # Setup AWS configuration
    log "INFO" "Setting up AWS configuration..."
    aws_config=$(setup_aws_config "$AWS_PROFILE" "$ACCOUNT_NUMBER")
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to setup AWS configuration"
        exit 1
    fi
    
    # Get and verify region
    log "INFO" "Getting AWS region configuration..."
    DEFAULT_REGION=$(aws configure get region 2>&1)
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to get AWS region: $DEFAULT_REGION"
        exit 1
    fi
    
    # Export region for use in other functions
    export AWS_DEFAULT_REGION="$DEFAULT_REGION"
    
    log "INFO" "AWS configuration completed:"
    log "INFO" "  - Account Number: $ACCOUNT_NUMBER"
    log "INFO" "  - Region: $DEFAULT_REGION"
    
    # Setup output directory
    setup_output_directory "$ACCOUNT_NUMBER"
    
    # Process each region
    log "INFO" "Processing region: $DEFAULT_REGION"
    
    # Retrieve metadata for the region
    log "INFO" "Retrieving metadata for region $DEFAULT_REGION..."
    metadata=$(retrieve_metadata "$DEFAULT_REGION" "$TABLE_PREFIX" "$TABLE_POSTFIX" "$USE_BOTH" "$PROCESS_ALL")
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to retrieve metadata for region $DEFAULT_REGION"
        exit 1
    fi
    
    # Parse metadata
    TOTAL_TABLES=$(echo "$metadata" | cut -d':' -f1)
    PARALLEL_PROCESSES=$(echo "$metadata" | cut -d':' -f2)
    INITIAL_CLOUDWATCH_CALLS=$(echo "$metadata" | cut -d':' -f3)
    ESTIMATED_SECONDS=$(echo "$metadata" | cut -d':' -f4)
    TABLES=$(echo "$metadata" | cut -d':' -f5)
    
    # Process tables
    process_tables "$DEFAULT_REGION" "$TABLES"
    
    # Display final summary
    show_final_summary
}

# Run main function
main "$@"
