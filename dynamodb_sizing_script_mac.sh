#!/bin/bash
#set -x

# ============================================================================
# DynamoDB and ScyllaDB Sizing Collection Script (Mac Version)
# This script collects comprehensive sizing and usage metrics for DynamoDB tables
# to help with capacity planning and performance analysis.
# ============================================================================

# ============================================================================
# Help Function
# Displays usage information and available options for the script
# ============================================================================
show_help() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -pre PREFIX    Filter tables that start with PREFIX"
    echo "  -post POSTFIX  Filter tables that end with POSTFIX"
    echo "  -both          Use both prefix and postfix filters (AND condition)"
    echo "  -all           Process all tables (overrides other filters)"
    echo "  -a ACCOUNT     AWS account number (optional)"
    echo "  -p PROFILE     AWS profile to use (optional)"
    echo "  -d DAYS        Number of days to collect data for (default: 7)"
    echo "Example with filters:"
    echo "  $0 -pre users -post -prod"
    echo "Example with AWS profile:"
    echo "  $0 -all -p my-aws-profile -a 123456789012"
    exit 1
}

# ============================================================================
# Parallel Process Calculation Function
# Determines the optimal number of parallel processes based on table count
# Uses different limits for even and odd numbers of tables
# ============================================================================
calculate_parallel_processes() {
    local num_tables=$1
    local max_even=6
    local max_odd=7
    
    # If number of tables is 1 or 2, return 1
    if [ "$num_tables" -le 2 ]; then
        echo 1
        return
    fi
    
    # Check if number is even or odd
    if [ $((num_tables % 2)) -eq 0 ]; then
        # Even number
        if [ "$num_tables" -le "$max_even" ]; then
            echo "$num_tables"
        else
            echo "$max_even"
        fi
    else
        # Odd number
        if [ "$num_tables" -le "$max_odd" ]; then
            echo "$num_tables"
        else
            echo "$max_odd"
        fi
    fi
}

# ============================================================================
# Script Initialization and Argument Parsing
# Checks for help flag and initializes variables
# ============================================================================
# Check for help flag
if [ "$#" -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    show_help
fi

# Initialize variables
TABLE_PREFIX=""
TABLE_POSTFIX=""
USE_BOTH=false
PROCESS_ALL=false
AWS_PROFILE=""
ACCOUNT_NUMBER=""
DAYS=7  # Default to 7 days

# Create a temporary directory for shared files
SHARED_DIR=$(mktemp -d)
trap 'rm -rf "$SHARED_DIR"' EXIT

# Create counter files
echo "0" > "$SHARED_DIR/cloudwatch_calls"
echo "0" > "$SHARED_DIR/processed_tables"

# Function to increment CloudWatch calls counter
increment_cloudwatch_calls() {
    local current_calls=$(cat "$SHARED_DIR/cloudwatch_calls")
    echo $((current_calls + 1)) > "$SHARED_DIR/cloudwatch_calls"
}

# Function to get current CloudWatch calls count
get_cloudwatch_calls() {
    cat "$SHARED_DIR/cloudwatch_calls"
}

# Function to increment processed tables counter
increment_processed_tables() {
    local current_tables=$(cat "$SHARED_DIR/processed_tables")
    echo $((current_tables + 1)) > "$SHARED_DIR/processed_tables"
}

# Function to get current processed tables count
get_processed_tables() {
    cat "$SHARED_DIR/processed_tables"
}

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
            echo "Error: Unknown option: $1"
            show_help
            ;;
    esac
done

# ============================================================================
# AWS Configuration
# Sets up AWS profile and retrieves account information
# ============================================================================
# Set AWS profile if provided
if [ -n "$AWS_PROFILE" ]; then
    echo "Using AWS profile: $AWS_PROFILE"
    export AWS_PROFILE="$AWS_PROFILE"
fi

# If account number not provided, try to get it from STS
if [ -z "$ACCOUNT_NUMBER" ]; then
    echo "No account number provided, attempting to retrieve from AWS..."
    ACCOUNT_INFO=$(aws sts get-caller-identity 2>/dev/null)
    if [ $? -eq 0 ]; then
        ACCOUNT_NUMBER=$(echo "$ACCOUNT_INFO" | jq -r '.Account')
        echo "Using account number: $ACCOUNT_NUMBER"
    else
        echo "Could not determine AWS account number. Please provide it with the -a flag."
        exit 1
    fi
fi

# Get the default region from AWS config
DEFAULT_REGION=$(aws configure get region)
if [ -z "$DEFAULT_REGION" ]; then
    DEFAULT_REGION="us-east-1"  # Default to us-east-1 if not set
fi

# ============================================================================
# AWS Credentials Verification
# Ensures AWS credentials are valid before proceeding
# ============================================================================
echo "Verifying AWS credentials..."
aws sts get-caller-identity > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Error: AWS credentials verification failed."
    echo "Please check that your credentials or IAM role has the necessary permissions."
    echo "If using an IAM role, ensure AWS_SESSION_TOKEN is properly set in your environment."
    exit 1
fi

echo "AWS credentials verified successfully."
echo "Using AWS region: $DEFAULT_REGION"

# ============================================================================
# Output Directory Setup
# Creates timestamped directory for output files
# ============================================================================
OUTPUT_DIR="dynamodb_metrics_${ACCOUNT_NUMBER}_$(date +"%Y%m%d_%H%M%S")"
mkdir -p "$OUTPUT_DIR"
SUMMARY_FILE="$OUTPUT_DIR/dynamodb_summary.csv"
DETAILED_FILE="$OUTPUT_DIR/dynamodb_detailed.json"

# CSV header
echo "Table Name,Avg Item Size (KB),Total Size (GB),Provisioned RCU,Provisioned WCU,Fourteen Day  Consumed RCU (Avg),Fourteen Day  WCU (Avg),2 week  Reads/Sec (Avg),2 week Writes/Sec (Avg),Read P99 Latency (ms),Write P99 Latency (ms),Streams Enabled,Stream View Type,LSI Count,GSI Count" >> "$OUTPUT_DIR/dynamodb_summary.csv"

# ============================================================================
# Filter Configuration Logging
# Logs the filter settings being used
# ============================================================================
echo "Filter configuration:" | tee -a "$OUTPUT_DIR/script.log"
if [ "$PROCESS_ALL" = true ]; then
    echo "- Processing ALL tables (ignoring other filters)" | tee -a "$OUTPUT_DIR/script.log"
    # Clear other filters when -all is specified
    TABLE_PREFIX=""
    TABLE_POSTFIX=""
    USE_BOTH=false
else
    if [ -n "$TABLE_PREFIX" ]; then
        echo "- Prefix filter: $TABLE_PREFIX" | tee -a "$OUTPUT_DIR/script.log"
    fi
    if [ -n "$TABLE_POSTFIX" ]; then
        echo "- Postfix filter: $TABLE_POSTFIX" | tee -a "$OUTPUT_DIR/script.log"
    fi
    if [ "$USE_BOTH" = true ]; then
        echo "- Using both prefix AND postfix (AND condition)" | tee -a "$OUTPUT_DIR/script.log"
    else
        if [ -n "$TABLE_PREFIX" ] && [ -n "$TABLE_POSTFIX" ]; then
            echo "- Using either prefix OR postfix (OR condition)" | tee -a "$OUTPUT_DIR/script.log"
        fi
    fi
    if [ -z "$TABLE_PREFIX" ] && [ -z "$TABLE_POSTFIX" ]; then
        echo "- No filters applied, collecting metrics for all tables" | tee -a "$OUTPUT_DIR/script.log"
    fi
fi

# ============================================================================
# Region Configuration
# Sets up regions to process
# ============================================================================
REGIONS="$DEFAULT_REGION"
echo "Only processing the default region: $REGIONS"

# ============================================================================
# Average Item Size Calculation Function
# Calculates the average size of items in a DynamoDB table
# ============================================================================
calculate_avg_item_size() {
    local table_name=$1
    local region=$2
    
    # Get table description
    table_info=$(aws dynamodb describe-table --table-name "$table_name" --region "$region")
    
    # Get item count and table size
    item_count=$(echo "$table_info" | jq -r '.Table.ItemCount')
    table_size_bytes=$(echo "$table_info" | jq -r '.Table.TableSizeBytes')
    
    # Calculate average item size in KB
    if [ "$item_count" -gt 0 ]; then
        avg_item_size=$(echo "scale=2; $table_size_bytes / $item_count / 1024" | bc)
    else
        avg_item_size=0
    fi
    
    echo "$avg_item_size"
}

# ============================================================================
# Period Calculation Function
# Determines appropriate period for CloudWatch metrics based on time window
# ============================================================================
get_period_for_window() {
    local seconds=$1
    if [ $seconds -le 10800 ]; then  # 3 hours
        echo "1"  # 1 second period
    elif [ $seconds -le 1296000 ]; then  # 15 days
        echo "60"  # 1 minute period
    else  # 45 days or more
        echo "300"  # 5 minute period
    fi
}

# ============================================================================
# DynamoDB Metrics Collection Function
# Collects CloudWatch metrics for a DynamoDB table using parallel processing
# ============================================================================
get_dynamodb_metrics() {
    local TABLE_NAME="$1"
    local REGION="$2"
    
    # Get table creation date
    local table_info=$(aws dynamodb describe-table --table-name "$TABLE_NAME" --region "$REGION")
    local creation_date=$(echo "$table_info" | jq -r '.Table.CreationDateTime')
    local creation_timestamp=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$creation_date" +%s)
    
    echo "Table $TABLE_NAME was created on: $(date -j -f "%s" "$creation_timestamp" '+%Y-%m-%d %H:%M:%S')"
  
    # Operations to monitor
    local READ_OPS=("GetItem" "BatchGetItem" "Scan")
    local WRITE_OPS=("PutItem" "BatchWriteItem")
  
    # Time windows to analyze (in seconds)
    if [ "$DAYS" -eq 45 ]; then
        local TIME_WINDOWS=("10800:3hours" "604800:7days" "3888000:45days")
        echo "Collecting data for all three time windows: 3 hours, 7 days, and 45 days"
    else
        local TIME_WINDOWS=("10800:3hours" "604800:7days")
        echo "Note: Only collecting data for 3 hours and 7 days windows to reduce CloudWatch API calls"
        echo "Total expected iterations:"
        echo "  3 hours window: 9 iterations (20-minute resolution, 1-second period)"
        echo "  7 days window: 336 iterations (30-minute resolution, 1-minute period)"
    fi

    # Function to make a single CloudWatch API call
    make_cloudwatch_call() {
        local op="$1"
        local window_start="$2"
        local window_end="$3"
        local period="$4"
        local output_file="$5"
        local call_number="$6"
        
        # Create a temporary file for this specific call
        local temp_file="${temp_dir}/${op}_call_${call_number}.json"
        
        # Convert window times to timestamps for comparison
        local window_start_timestamp=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_start" +%s)
        local window_end_timestamp=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_end" +%s)
        
        # Skip if the entire window is before table creation
        if [ $window_end_timestamp -lt $creation_timestamp ]; then
            echo "Skipping CloudWatch call for period before table creation: $window_start to $window_end"
            return
        fi
        
        # Adjust window start if it's before table creation
        if [ $window_start_timestamp -lt $creation_timestamp ]; then
            window_start=$(date -j -f "%s" "$creation_timestamp" '+%Y-%m-%d %H:%M:%S')
            echo "Adjusted window start to table creation time: $window_start"
        fi
        
        # Ensure window times are in ISO 8601 format for CloudWatch
        window_start=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_start" '+%Y-%m-%dT%H:%M:%S')
        window_end=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_end" '+%Y-%m-%dT%H:%M:%S')
        
        echo "Making CloudWatch call #${call_number} for DynamoDB API: ${op}RequestCount"
        echo "  - Table: $TABLE_NAME"
        echo "  - Period: $period seconds"
        echo "  - Time window: $window_start to $window_end"
        
        # Make the CloudWatch API call with extended statistics
        local cloudwatch_output=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ${op}RequestCount \
            --start-time "$window_start" \
            --end-time "$window_end" \
            --period "$period" \
            --statistics SampleCount \
            --dimensions Name=TableName,Value="$TABLE_NAME" \
            --region "$REGION" \
            --output json 2>&1)
            
        # Increment the shared CloudWatch calls counter
        increment_cloudwatch_calls
        
        # Check for AWS CLI errors
        if [ $? -ne 0 ]; then
            echo "Error making CloudWatch API call #${call_number}: $cloudwatch_output"
            return
        fi
        
        # Check if we got any data points
        local datapoint_count=$(echo "$cloudwatch_output" | jq '.Datapoints | length')
        if [ "$datapoint_count" -eq 0 ]; then
            echo "No data points found for call #${call_number} period: $window_start to $window_end"
            echo "Metric: ${op}RequestCount"
            echo "Period: $period"
            echo "Table: $TABLE_NAME"
            return
        fi
        
        # Process and format the output
        local formatted_output=$(echo "$cloudwatch_output" | jq -r '.Datapoints[] | {
            Timestamp: .Timestamp,
            SampleCount: .SampleCount
        }')
        
        # Check if jq processing was successful
        if [ $? -ne 0 ]; then
            echo "Error processing CloudWatch output with jq for call #${call_number}"
            return
        fi
        
        # Write to temporary file only if we have valid data
        if [ -n "$formatted_output" ]; then
            echo "$formatted_output" > "$temp_file"
            echo "Retrieved $datapoint_count data points for call #${call_number} ($op)"
        else
            echo "Warning: No valid data points after processing for call #${call_number} ($op)"
        fi
    }
  
    # Process each time window
    for window in "${TIME_WINDOWS[@]}"; do
        # Split the window string into seconds and name
        IFS=":" read -r seconds name <<< "$window"
        
        # Get appropriate period for this time window
        local period=$(get_period_for_window $seconds)
        
        # Calculate start time based on the window
        local end_time=$(date '+%Y-%m-%dT%H:%M:%S')
        local start_time=$(date -j -v-${seconds}S '+%Y-%m-%dT%H:%M:%S')
        
        # Check if the entire time window is before table creation
        local window_start_timestamp=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$start_time" +%s)
        local window_end_timestamp=$(date -j -f "%Y-%m-%dT%H:%M:%S" "$end_time" +%s)
        
        # Skip if the entire window is before table creation
        if [ $window_end_timestamp -lt $creation_timestamp ]; then
            echo "Skipping entire $name window as it's before table creation"
            continue
        fi
        
        # Adjust window start if it's before table creation
        if [ $window_start_timestamp -lt $creation_timestamp ]; then
            start_time=$(date -j -f "%s" "$creation_timestamp" '+%Y-%m-%d %H:%M:%S')
            echo "Adjusted window start to table creation time: $start_time"
        fi
        
        echo "Processing time window: $name ($start_time to $end_time) with period: ${period}s"
        
        # Process read operations
        for op in "${READ_OPS[@]}"; do
            local output_file="$OUTPUT_DIR/${TABLE_NAME}_${op}_${name}.json"
            echo "Retrieving $op metrics for $name window..."
            
            # Initialize variables for the time window
            local end_time="$(date '+%Y-%m-%d %H:%M:%S')"
            local x=0  # Start from 0
            local call_counter=0  # Counter for sequential call numbering
            
            # Adjust increment and loop conditions based on time window
            if [ "$seconds" -eq 10800 ]; then  # 3 hours
                local max_x=10800
                local period=1
                local increment=1200  # 20 minutes, exactly 9 iterations for 3 hours
                local max_parallel=9  # Process all 3-hour window calls in parallel
            elif [ "$seconds" -eq 604800 ]; then  # 7 days
                local max_x=604800
                local period=60
                local increment=1800  # 30 minutes, 336 iterations for 7 days
                local max_parallel=100  # Process up to 100 calls in parallel
            else  # 45 days
                local max_x=3888000
                local period=300
                local increment=5400  # 90 minutes, 720 iterations for 45 days
                local max_parallel=100  # Process up to 100 calls in parallel
            fi
            
            # Create a temporary directory for parallel processing
            local temp_dir=$(mktemp -d)
            trap 'rm -rf "$temp_dir"' EXIT
            
            # Process in parallel batches
            while [ $x -lt $max_x ]; do
                local batch_count=0
                local pids=()
                local total_calls_in_batch=0
                
                # Start a batch of parallel processes
                while [ $batch_count -lt $max_parallel ] && [ $x -lt $max_x ]; do
                    # Calculate window start and end times
                    local window_end="$(date -j -v-${x}S '+%Y-%m-%d %H:%M:%S')"
                    local window_start="$(date -j -v-$((x + increment))S '+%Y-%m-%d %H:%M:%S')"
                    
                    # Convert to timestamps for comparison
                    local window_start_ts=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_start" +%s)
                    local window_end_ts=$(date -j -f "%Y-%m-%d %H:%M:%S" "$window_end" +%s)
                    
                    # Skip if the entire window is before table creation
                    if [ $window_end_ts -lt $creation_timestamp ]; then
                        echo "Skipping CloudWatch call for period before table creation: $window_start to $window_end"
                        x=$((x + increment))
                        continue
                    fi
                    
                    # Adjust window start if it's before table creation
                    if [ $window_start_ts -lt $creation_timestamp ]; then
                        window_start=$(date -j -f "%s" "$creation_timestamp" '+%Y-%m-%d %H:%M:%S')
                        echo "Adjusted window start to table creation time: $window_start"
                    fi
                    
                    # Increment call counter
                    call_counter=$((call_counter + 1))
                    total_calls_in_batch=$((total_calls_in_batch + 1))
                    
                    # Make the CloudWatch call in the background
                    make_cloudwatch_call "$op" "$window_start" "$window_end" "$period" "$output_file" "$call_counter" &
                    pids+=($!)
                    
                    x=$((x + increment))
                    batch_count=$((batch_count + 1))
                    
                    # Check if we've reached 100 calls
                    if [ $total_calls_in_batch -eq 100 ]; then
                        echo "========================================================"
                        echo "Pausing after 100 CloudWatch API calls..."
                        echo "Waiting for all current calls to complete before continuing..."
                        echo "========================================================"
                        # Wait for all current processes to complete
                        for pid in "${pids[@]}"; do
                            wait $pid
                        done
                        # Reset counters for next batch
                        pids=()
                        total_calls_in_batch=0
                        # Add a small delay to respect rate limits
                        sleep 2
                    fi
                done
                
                # Wait for the current batch to complete
                for pid in "${pids[@]}"; do
                    wait $pid
                done
                
                echo "Completed batch of $batch_count CloudWatch calls for table $TABLE_NAME, operation $op, window $name"
            done
            
            # Concatenate non-empty temporary files into the final output file
            echo "Concatenating results for $op..."
            for temp_file in $(ls -v "${temp_dir}/${op}_call_"*.json 2>/dev/null); do
                if [ -s "$temp_file" ]; then
                    cat "$temp_file" >> "$output_file"
                    echo "" >> "$output_file"  # Add newline between files
                fi
            done
            
            # Clean up
            rm -rf "$temp_dir"
        done
        
        # Process write operations (similar changes as above)
        for op in "${WRITE_OPS[@]}"; do
            # ... (same changes as above for write operations)
        done
    done
    
    # Add consumed capacity metrics calls
    increment_cloudwatch_calls  # For ConsumedReadCapacityUnits
    increment_cloudwatch_calls  # For ConsumedWriteCapacityUnits
    
    echo "Total CloudWatch API calls for table $TABLE_NAME: $(get_cloudwatch_calls)"
    CLOUDWATCH_CALLS_PER_TABLE=$(get_cloudwatch_calls)
}

# ============================================================================
# Status Update Function
# Displays periodic status updates about script progress
# ============================================================================
show_status_update() {
    local current_time=$(date +%s)
    local elapsed_time=$((current_time - START_TIME))
    local hours=$((elapsed_time / 3600))
    local minutes=$(((elapsed_time % 3600) / 60))
    local seconds=$((elapsed_time % 60))
    local current_cloudwatch_calls=$(get_cloudwatch_calls)
    local processed_tables=$(get_processed_tables)
    
    echo "========================================================"
    echo "Status Update at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "--------------------------------------------------------"
    echo "Progress:"
    echo "  - Tables processed: $processed_tables of $TOTAL_TABLES"
    echo "  - Tables skipped: $TABLES_SKIPPED"
    echo "  - Remaining tables: $((TOTAL_TABLES - processed_tables))"
    echo "  - Current table: $TABLE"
    echo "  - Current operation: $op"
    echo "  - Current time window: $name"
    echo "--------------------------------------------------------"
    echo "Performance:"
    echo "  - Elapsed time: ${hours}h ${minutes}m ${seconds}s"
    echo "  - CloudWatch calls made: $current_cloudwatch_calls"
    echo "  - Average calls per table: $(echo "scale=2; $current_cloudwatch_calls / ($processed_tables + 1)" | bc)"
    echo "  - Processing rate: $(echo "scale=2; $processed_tables / ($elapsed_time / 60)" | bc) tables/minute"
    echo "========================================================"
}

# Start the status update timer
LAST_STATUS_UPDATE=$(date +%s)
STATUS_UPDATE_INTERVAL=20  # Update every 20 seconds

# ============================================================================
# Main Processing Loop
# Processes each region and its tables
# ============================================================================
# Process each region
for REGION in $REGIONS; do
    echo "========================================================"
    echo "Starting processing for region: $REGION"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================================"
    
    # Get list of tables in the region
    echo "Retrieving list of tables from region $REGION..."
    TABLES=$(aws dynamodb list-tables --region "$REGION" --query "TableNames" --output text)
    
    # Skip if no tables found
    if [ -z "$TABLES" ]; then
        echo "No tables found in region $REGION"
        continue
    fi
    
    # Count total tables
    TOTAL_TABLES=$(echo "$TABLES" | wc -w)
    echo "Found $TOTAL_TABLES tables in region $REGION"
    
    # Calculate optimal number of parallel processes
    PARALLEL_PROCESSES=$(calculate_parallel_processes "$TOTAL_TABLES")
    echo "Parallel Processing Configuration:"
    echo "  - Optimal parallel processes: $PARALLEL_PROCESSES"
    echo "  - Tables per process: $(echo "scale=2; $TOTAL_TABLES / $PARALLEL_PROCESSES" | bc)"
    
    # Calculate initial estimated CloudWatch calls per table
    INITIAL_CLOUDWATCH_CALLS=0
    if [ "$DAYS" -eq 45 ]; then
        # 3 hours window: 9 iterations * (3 read ops + 2 write ops)
        # 7 days window: 336 iterations * (3 read ops + 2 write ops)
        # 45 days window: 720 iterations * (3 read ops + 2 write ops)
        # Plus 2 calls for consumed capacity metrics
        INITIAL_CLOUDWATCH_CALLS=$((9 * 5 + 336 * 5 + 720 * 5 + 2))
    else
        # 3 hours window: 9 iterations * (3 read ops + 2 write ops)
        # 7 days window: 336 iterations * (3 read ops + 2 write ops)
        # Plus 2 calls for consumed capacity metrics
        INITIAL_CLOUDWATCH_CALLS=$((9 * 5 + 336 * 5 + 2))
    fi
    
    # Calculate initial estimated completion time with parallel processing
    # Each CloudWatch call takes about 3 seconds, but we can make them in parallel
    # We'll estimate based on the number of parallel processes and API rate limits
    estimated_seconds=$((TOTAL_TABLES * INITIAL_CLOUDWATCH_CALLS * 3 / PARALLEL_PROCESSES))
    # Add buffer for API rate limits (assuming 100 calls per minute limit)
    rate_limit_buffer=$((TOTAL_TABLES * INITIAL_CLOUDWATCH_CALLS / 100 * 60))
    estimated_seconds=$((estimated_seconds + rate_limit_buffer))
    estimated_hours=$((estimated_seconds / 3600))
    estimated_minutes=$(((estimated_seconds % 3600) / 60))
    
    echo "Initial Performance Estimates:"
    echo "  - CloudWatch calls per table: $INITIAL_CLOUDWATCH_CALLS"
    echo "  - Total estimated CloudWatch calls: $((TOTAL_TABLES * INITIAL_CLOUDWATCH_CALLS))"
    echo "  - Estimated completion time: ${estimated_hours}h ${estimated_minutes}m"
    echo "  - Average processing time per table: $(echo "scale=2; $estimated_seconds / $TOTAL_TABLES / 60" | bc) minutes"
    echo "  - Parallel processing factor: $PARALLEL_PROCESSES"
    echo "  - Rate limit buffer time: $(echo "scale=2; $rate_limit_buffer / 60" | bc) minutes"
    
    # Create a temporary directory for parallel processing
    TEMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TEMP_DIR"' EXIT
    
    # Initialize counters and timers
    PROCESSED_TABLES=0
    START_TIME=$(date +%s)
    LAST_UPDATE_TIME=$START_TIME
    TABLES_SKIPPED=0
    
    echo "========================================================"
    echo "Starting table processing at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================================"
    
    # Process tables in parallel
    for TABLE in $TABLES; do
        # Check if it's time for a status update
        current_time=$(date +%s)
        if [ $((current_time - LAST_STATUS_UPDATE)) -ge $STATUS_UPDATE_INTERVAL ]; then
            show_status_update
            LAST_STATUS_UPDATE=$current_time
        fi
        
        # Apply table filters if provided (skip if -all flag is set)
        SKIP_TABLE=false
        
        if [ "$PROCESS_ALL" = false ]; then
            if [ "$USE_BOTH" = true ]; then
                # AND condition: Must match both prefix AND postfix
                if [ -n "$TABLE_PREFIX" ] && [ -n "$TABLE_POSTFIX" ]; then
                    if [[ "$TABLE" != "$TABLE_PREFIX"* || "$TABLE" != *"$TABLE_POSTFIX" ]]; then
                        echo "Skipping table $TABLE (doesn't match both prefix $TABLE_PREFIX AND postfix $TABLE_POSTFIX)"
                        SKIP_TABLE=true
                        TABLES_SKIPPED=$((TABLES_SKIPPED + 1))
                    fi
                fi
            else
                # OR condition: Check each filter independently
                if [ -n "$TABLE_PREFIX" ]; then
                    if [[ "$TABLE" != "$TABLE_PREFIX"* ]]; then
                        echo "Skipping table $TABLE (doesn't match prefix filter $TABLE_PREFIX)"
                        SKIP_TABLE=true
                        TABLES_SKIPPED=$((TABLES_SKIPPED + 1))
                    fi
                fi
                
                if [ -n "$TABLE_POSTFIX" ] && [ "$SKIP_TABLE" = false ]; then
                    if [[ "$TABLE" != *"$TABLE_POSTFIX" ]]; then
                        echo "Skipping table $TABLE (doesn't match postfix filter $TABLE_POSTFIX)"
                        SKIP_TABLE=true
                        TABLES_SKIPPED=$((TABLES_SKIPPED + 1))
                    fi
                fi
            fi
        fi
        
        # Skip this table if it didn't pass the filters
        if [ "$SKIP_TABLE" = true ]; then
            continue
        fi
        
        echo "========================================================"
        echo "Starting processing for table: $TABLE"
        echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "========================================================"
        
        # Create a temporary file for this table's output
        TABLE_OUTPUT="$TEMP_DIR/${TABLE}_output.txt"
        
        # Process the table in the background
        (
            # Display banner for table processing
            echo "========================================================"
            echo "Processing DynamoDB Table: $TABLE"
            echo "AWS Account: $ACCOUNT_NUMBER"
            echo "Region: $REGION"
            echo "Start Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "========================================================"
        
            # Get table description
            echo "Retrieving table description..."
            TABLE_INFO=$(aws dynamodb describe-table --table-name "$TABLE" --region "$REGION")
            
            # Extract table size in GB
            TABLE_SIZE_BYTES=$(echo "$TABLE_INFO" | jq -r '.Table.TableSizeBytes')
            TABLE_SIZE_GB=$(echo "scale=2; $TABLE_SIZE_BYTES / (1024*1024*1024)" | bc)
            echo "Table Size: ${TABLE_SIZE_GB}GB"
            
            # Calculate average item size
            echo "Calculating average item size..."
            AVG_ITEM_SIZE=$(calculate_avg_item_size "$TABLE" "$REGION")
            echo "Average Item Size: ${AVG_ITEM_SIZE}KB"
            
            # Get provisioned capacity
            BILLING_MODE=$(echo "$TABLE_INFO" | jq -r '.Table.BillingModeSummary.BillingMode // "PROVISIONED"')
            echo "Billing Mode: $BILLING_MODE"
            
            if [ "$BILLING_MODE" == "PROVISIONED" ]; then
                PROV_RCU=$(echo "$TABLE_INFO" | jq -r '.Table.ProvisionedThroughput.ReadCapacityUnits')
                PROV_WCU=$(echo "$TABLE_INFO" | jq -r '.Table.ProvisionedThroughput.WriteCapacityUnits')
                echo "Provisioned Capacity:"
                echo "  - Read Capacity Units: $PROV_RCU"
                echo "  - Write Capacity Units: $PROV_WCU"
            else
                PROV_RCU="On-Demand"
                PROV_WCU="On-Demand"
                echo "On-Demand Capacity"
            fi
            
            # Check for stream configuration
            STREAM_ENABLED=$(echo "$TABLE_INFO" | jq -r '.Table.StreamSpecification.StreamEnabled // "false"')
            if [ "$STREAM_ENABLED" == "true" ]; then
                STREAM_VIEW_TYPE=$(echo "$TABLE_INFO" | jq -r '.Table.StreamSpecification.StreamViewType')
                echo "DynamoDB Streams: Enabled ($STREAM_VIEW_TYPE)"
            else
                STREAM_VIEW_TYPE="N/A"
                echo "DynamoDB Streams: Disabled"
            fi
            
            # Get LSI and GSI information
            LSI_COUNT=$(echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes | length // 0')
            GSI_COUNT=$(echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes | length // 0')
            echo "Indexes:"
            echo "  - Local Secondary Indexes: $LSI_COUNT"
            echo "  - Global Secondary Indexes: $GSI_COUNT"
            
            # Initialize GSI capacity variables
            GSI_INFO=""
            
            # Extract GSI provisioned capacity if GSIs exist
            if [ "$GSI_COUNT" -gt 0 ]; then
                echo "Processing Global Secondary Indexes..."
                GSI_INFO="GSI Details: "
                GSI_INDEXES=$(echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes[]')
                
                # Loop through each GSI and extract capacity information
                echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes[] | .IndexName' | while read -r GSI_NAME; do
                    if [ "$BILLING_MODE" == "PROVISIONED" ]; then
                        GSI_RCU=$(echo "$TABLE_INFO" | jq -r ".Table.GlobalSecondaryIndexes[] | select(.IndexName == \"$GSI_NAME\") | .ProvisionedThroughput.ReadCapacityUnits")
                        GSI_WCU=$(echo "$TABLE_INFO" | jq -r ".Table.GlobalSecondaryIndexes[] | select(.IndexName == \"$GSI_NAME\") | .ProvisionedThroughput.WriteCapacityUnits")
                        GSI_INFO="$GSI_INFO [$GSI_NAME: RCU=$GSI_RCU, WCU=$GSI_WCU]"
                        echo "  - GSI $GSI_NAME: RCU=$GSI_RCU, WCU=$GSI_WCU"
                    else
                        GSI_INFO="$GSI_INFO [$GSI_NAME: On-Demand]"
                        echo "  - GSI $GSI_NAME: On-Demand"
                    fi
                done
            fi
            
            echo "Starting CloudWatch metrics collection..."
            get_dynamodb_metrics "$TABLE" "$REGION"
            
            # Calculate and display estimated completion time for remaining tables
            CURRENT_TIME=$(date +%s)
            ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
            REMAINING_TABLES=$((TOTAL_TABLES - PROCESSED_TABLES - 1))
            
            if [ $PROCESSED_TABLES -gt 0 ]; then
                # Calculate average time per table
                AVG_TIME_PER_TABLE=$((ELAPSED_TIME / PROCESSED_TABLES))
                # Estimate remaining time
                ESTIMATED_REMAINING_SECONDS=$((AVG_TIME_PER_TABLE * REMAINING_TABLES / PARALLEL_PROCESSES))
            else
                # Use initial estimate for first table
                ESTIMATED_REMAINING_SECONDS=$((REMAINING_TABLES * INITIAL_CLOUDWATCH_CALLS * 3 / PARALLEL_PROCESSES))
            fi
            
            ESTIMATED_HOURS=$((ESTIMATED_REMAINING_SECONDS / 3600))
            ESTIMATED_MINUTES=$(((ESTIMATED_REMAINING_SECONDS % 3600) / 60))
            
            # Check if bc is available
            if ! command -v bc &> /dev/null; then
                echo "Warning: bc command not found. Using integer arithmetic for calculations."
                # Function to perform integer division with rounding
                int_divide() {
                    local dividend=$1
                    local divisor=$2
                    local scale=$3
                    local result=$(( (dividend * 10**scale) / divisor ))
                    echo "${result:0:-scale}.${result: -scale}"
                }
                
                # Replace bc calculations with integer arithmetic
                TABLES_PER_MINUTE=$(int_divide $PROCESSED_TABLES $((ELAPSED_TIME / 60)) 2)
                AVG_TIME_PER_MINUTE=$(int_divide $AVG_TIME_PER_TABLE 60 2)
                AVG_CALLS_PER_TABLE=$(int_divide $TOTAL_CLOUDWATCH_CALLS $((PROCESSED_TABLES + 1)) 2)
            else
                # Use bc for floating-point calculations
                TABLES_PER_MINUTE=$(echo "scale=2; $PROCESSED_TABLES / ($ELAPSED_TIME / 60)" | bc)
                AVG_TIME_PER_MINUTE=$(echo "scale=2; $AVG_TIME_PER_TABLE / 60" | bc)
                AVG_CALLS_PER_TABLE=$(echo "scale=2; $TOTAL_CLOUDWATCH_CALLS / ($PROCESSED_TABLES + 1)" | bc)
            fi
            
            echo "========================================================"
            echo "Progress Update:"
            echo "  Tables processed: $((PROCESSED_TABLES + 1)) of $TOTAL_TABLES"
            echo "  Tables skipped: $TABLES_SKIPPED"
            echo "  Remaining tables: $REMAINING_TABLES"
            echo "  Processing rate: ${TABLES_PER_MINUTE} tables/minute"
            echo "  Average time per table: ${AVG_TIME_PER_MINUTE} minutes"
            echo "  Estimated time remaining: ${ESTIMATED_HOURS}h ${ESTIMATED_MINUTES}m"
            echo "  CloudWatch calls made: $TOTAL_CLOUDWATCH_CALLS"
            echo "  Average calls per table: ${AVG_CALLS_PER_TABLE}"
            echo "========================================================"
            
            # Create detailed JSON for the table
            TABLE_DETAILED=$(cat << EOF
{
    "TableName": "$TABLE",
    "Region": "$REGION",
    "TableSizeGB": $TABLE_SIZE_GB,
    "AverageItemSize": $AVG_ITEM_SIZE,
    "BillingMode": "$BILLING_MODE",
    "ProvisionedReadCapacity": "$PROV_RCU",
    "ProvisionedWriteCapacity": "$PROV_WCU",
    "AverageConsumedReadCapacity": $AVG_RCU,
    "AverageConsumedWriteCapacity": $AVG_WCU,
    "StreamEnabled": $STREAM_ENABLED,
    "StreamViewType": "$STREAM_VIEW_TYPE",
    "LSICount": $LSI_COUNT,
    "GSICount": $GSI_COUNT,
    "GSIInfo": "$GSI_INFO"
}
EOF
)
            
            # Save detailed table information
            echo "$TABLE_DETAILED" > "$OUTPUT_DIR/${TABLE}_detailed.json"
            
            # Create LSI file if LSIs exist
            if [ "$LSI_COUNT" -gt 0 ]; then
                echo "Creating LSI configuration file..."
                LSI_FILE="$OUTPUT_DIR/${TABLE}_lsi.json"
                echo "{" > "$LSI_FILE"
                echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes[] | "  \"\(.IndexName)\": {"' >> "$LSI_FILE"
                echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes[] | "    \"Projection\": \"\(.Projection.ProjectionType)\","' >> "$LSI_FILE"
                echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes[] | "    \"KeySchema\": \(.KeySchema)"' >> "$LSI_FILE"
                echo "  }," >> "$LSI_FILE"
                echo "}" >> "$LSI_FILE"
            fi
            
            # Create summary line for the table
            echo "$TABLE,$AVG_ITEM_SIZE,$TABLE_SIZE_GB,$PROV_RCU,$PROV_WCU,$AVG_RCU,$AVG_WCU,$STREAM_ENABLED,$STREAM_VIEW_TYPE,$LSI_COUNT,$GSI_COUNT" >> "$SUMMARY_FILE"
            
            # Increment processed tables counter
            increment_processed_tables
            
            echo "========================================================"
            echo "Completed processing table: $TABLE"
            echo "End Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "========================================================"
            
        ) > "$TABLE_OUTPUT" 2>&1 &
        
        # Limit the number of parallel processes
        while [ $(jobs -p | wc -l) -ge "$PARALLEL_PROCESSES" ]; do
            sleep 1
            # Check if it's time for a status update
            current_time=$(date +%s)
            if [ $((current_time - LAST_STATUS_UPDATE)) -ge $STATUS_UPDATE_INTERVAL ]; then
                show_status_update
                LAST_STATUS_UPDATE=$current_time
                # Force flush of any buffered output
                sync
            fi
        done
    done
    
    # Wait for all background processes to complete
    wait
    
    # Count processed tables
    PROCESSED_TABLES=$(ls "$TEMP_DIR"/*_processed 2>/dev/null | wc -l)
    
    # Combine all outputs
    cat "$TEMP_DIR"/*_output.txt
    
    # Clean up
    rm -rf "$TEMP_DIR"
    
    echo "========================================================"
    echo "Region Processing Summary:"
    echo "  Region: $REGION"
    echo "  Total tables found: $TOTAL_TABLES"
    echo "  Tables processed: $PROCESSED_TABLES"
    echo "  Tables skipped: $TABLES_SKIPPED"
    echo "  Total CloudWatch calls: $TOTAL_CLOUDWATCH_CALLS"
    echo "  Average calls per table: $(echo "scale=2; $TOTAL_CLOUDWATCH_CALLS / $PROCESSED_TABLES" | bc)"
    echo "========================================================"
done

# ============================================================================
# Final Summary
# Displays execution statistics and results
# ============================================================================
echo "========================================================"
echo "Script Execution Summary"
echo "========================================================"
echo "Total tables processed: $PROCESSED_TABLES"
echo "Total tables skipped: $TABLES_SKIPPED"
echo "Total CloudWatch API calls made: $TOTAL_CLOUDWATCH_CALLS"
echo "Average CloudWatch calls per table: $(echo "scale=2; $TOTAL_CLOUDWATCH_CALLS / $PROCESSED_TABLES" | bc)"

# Calculate actual execution time
END_TIME=$(date +%s)
TOTAL_EXECUTION_TIME=$((END_TIME - START_TIME))
TOTAL_HOURS=$((TOTAL_EXECUTION_TIME / 3600))
TOTAL_MINUTES=$(((TOTAL_EXECUTION_TIME % 3600) / 60))

echo "Performance Metrics:"
echo "  Total execution time: ${TOTAL_HOURS}h ${TOTAL_MINUTES}m"
echo "  Average processing time per table: $(echo "scale=2; $TOTAL_EXECUTION_TIME / $PROCESSED_TABLES / 60" | bc) minutes"
echo "  Processing rate: $(echo "scale=2; $PROCESSED_TABLES / ($TOTAL_EXECUTION_TIME / 60)" | bc) tables/minute"
echo "  Actual completion time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================================" 