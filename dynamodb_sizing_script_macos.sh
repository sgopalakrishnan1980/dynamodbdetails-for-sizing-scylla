#!/bin/bash
#set -x
# DynamoDB and ScyllaDB Sizing Collection Script for macOS
# This script collects sizing and usage metrics for DynamoDB tables

# Help function
show_help() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -pre PREFIX    Filter tables that start with PREFIX"
    echo "  -post POSTFIX  Filter tables that end with POSTFIX"
    echo "  -both          Use both prefix and postfix filters (AND condition)"
    echo "  -all           Process all tables (overrides other filters)"
    echo "  -a ACCOUNT     AWS account number (optional)"
    echo "  -p PROFILE     AWS profile to use (optional)"
    echo "  -d DAYS        Number of days to collect data for (default: 15)"
    echo "Example with filters:"
    echo "  $0 -pre users -post -prod"
    echo "Example with AWS profile:"
    echo "  $0 -all -p my-aws-profile -a 123456789012"
    exit 1
}

# Function to calculate optimal number of parallel processes
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
DAYS=15  # Default to 15 days
TOTAL_CLOUDWATCH_CALLS=0
CLOUDWATCH_CALLS_PER_TABLE=0

# Parse arguments
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

# Verify AWS credentials work before proceeding
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

OUTPUT_DIR="dynamodb_metrics_${ACCOUNT_NUMBER}_$(date +"%Y%m%d_%H%M%S")"
mkdir -p "$OUTPUT_DIR"
SUMMARY_FILE="$OUTPUT_DIR/dynamodb_summary.csv"
DETAILED_FILE="$OUTPUT_DIR/dynamodb_detailed.json"

# CSV header
echo "Table Name,Avg Item Size (KB),Total Size (GB),Provisioned RCU,Provisioned WCU,Fourteen Day  Consumed RCU (Avg),Fourteen Day  WCU (Avg),2 week  Reads/Sec (Avg),2 week Writes/Sec (Avg),Read P99 Latency (ms),Write P99 Latency (ms),Streams Enabled,Stream View Type,LSI Count,GSI Count" >> "$OUTPUT_DIR/dynamodb_summary.csv"

# Log the filters being used
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

# Get all regions
REGIONS="$DEFAULT_REGION"
echo "Only processing the default region: $REGIONS"

# Function to calculate average item size
calculate_avg_item_size() {
    local table_name=$1
    local region=$2
    
    # Get table description
    table_info=$(aws dynamodb describe-table --table-name "$table_name" --region "$region")
    
    # Get item count and table size
    item_count=$(echo "$table_info" | jq -r '.Table.ItemCount')
    table_size_bytes=$(echo "$table_info" | jq -r '.Table.TableSizeBytes')
    
    # Calculate average item size in KB with 2 decimal places using bc
    # bc is used for precise decimal arithmetic
    if [ "$item_count" -gt 0 ]; then
        avg_item_size=$(echo "scale=2; $table_size_bytes / $item_count / 1024" | bc)
    else
        avg_item_size=0
    fi
    
    echo "$avg_item_size"
}

# Function to determine appropriate period based on time window
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

# Function to get DynamoDB metrics
get_dynamodb_metrics() {
    local TABLE_NAME="$1"
    local REGION="$2"
    local local_cloudwatch_calls=0
  
    # Operations to monitor
    local READ_OPS=("GetItem" "BatchGetItem" "Scan")
    local WRITE_OPS=("PutItem" "BatchWriteItem")
  
    # Time windows to analyze (in seconds)
    if [ "$DAYS" -eq 45 ]; then
        local TIME_WINDOWS=("10800:3hours" "1296000:15days" "3888000:45days")
        echo "Collecting data for all three time windows: 3 hours, 15 days, and 45 days"
    else
        local TIME_WINDOWS=("10800:3hours" "1296000:15days")
        echo "Note: Only collecting data for 3 hours and 15 days windows to reduce CloudWatch API calls"
        echo "Total expected iterations:"
        echo "  3 hours window: 9 iterations (20-minute resolution, 1-second period)"
        echo "  15 days window: 720 iterations (30-minute resolution, 1-minute period)"
    fi
  
    for window in "${TIME_WINDOWS[@]}"; do
        # Split the window string into seconds and name
        IFS=":" read -r seconds name <<< "$window"
        
        # Get appropriate period for this time window
        local period=$(get_period_for_window $seconds)
        
        # Calculate start time based on the window
        local end_time=$(date '+%Y-%m-%dT%H:%M:%S')
        local start_time=$(date -v-${seconds}S '+%Y-%m-%dT%H:%M:%S')
        
        echo "Processing time window: $name ($start_time to $end_time) with period: ${period}s"
        
        # Process read operations
        for op in "${READ_OPS[@]}"; do
            touch "$OUTPUT_DIR"/"${TABLE_NAME}_${op}_${name}.json"
            output_file="./$OUTPUT_DIR"/"${TABLE_NAME}_${op}_${name}.json"
            echo "Retrieving $op metrics for $name window..."
            
            # Initialize variables for the time window
            local end_time="$(date '+%Y-%m-%d %H:%M:%S')"
            local x=0  # Start from 0
            
            # Adjust increment and loop conditions based on time window
            if [ "$seconds" -eq 10800 ]; then  # 3 hours
                local max_x=10800
                local period=1
                local increment=1200  # 20 minutes, exactly 9 iterations for 3 hours
            elif [ "$seconds" -eq 1296000 ]; then  # 15 days
                local max_x=1296000
                local period=60
                local increment=1800  # 30 minutes, 720 iterations for 15 days
            else  # 45 days
                local max_x=3888000
                local period=300
                local increment=5400  # 90 minutes, 720 iterations for 45 days
            fi
            
            # Ensure we do not exceed 1440 data points
            local n_points=$((max_x / increment))
            if [ $n_points -gt 1440 ]; then
                echo "Warning: More than 1440 data points would be retrieved. Adjusting increment."
                increment=$(( (max_x + 1439) / 1440 ))
            fi
            
            while [ $x -lt $max_x ]; do
                # Calculate window start and end times using macOS date command
                local window_end="$(date -v-${x}S '+%Y-%m-%d %H:%M:%S')"
                local window_start="$(date -v-$(($x + $increment))S '+%Y-%m-%d %H:%M:%S')"
                
                echo "Requesting CloudWatch metrics for $op:"
                echo "  Start Time: $window_start"
                echo "  End Time: $window_end"
                echo "  Period: ${period}s"
                echo "  Resolution: ${increment}s"
                
                aws cloudwatch get-metric-statistics \
                --namespace AWS/DynamoDB \
                --metric-name ${op}RequestCount \
                --start-time "$window_start" \
                --end-time "$window_end" \
                --period "$period" \
                --statistics Sum \
                --dimensions Name=TableName,Value="$TABLE_NAME" \
                --region "$REGION" \
                --query 'Datapoints[*].{Timestamp:Timestamp,RequestCount:Sum}' \
                --output json >> "$output_file"
                
                x=$((x + increment))
                local_cloudwatch_calls=$((local_cloudwatch_calls + 1))
                echo "Saved to $output_file"
            done
        done
        
        # Process write operations
        for op in "${WRITE_OPS[@]}"; do
            output_file="$OUTPUT_DIR/${TABLE_NAME}_${op}_${name}.json"
            echo "Retrieving $op metrics for $name window..."
            
            # Initialize variables for the time window
            local end_time="$(date '+%Y-%m-%d %H:%M:%S')"
            local x=0  # Start from 0
            
            # Adjust increment and loop conditions based on time window
            if [ "$seconds" -eq 10800 ]; then  # 3 hours
                local max_x=10800
                local period=1
                local increment=1200  # 20 minutes, exactly 9 iterations for 3 hours
            elif [ "$seconds" -eq 1296000 ]; then  # 15 days
                local max_x=1296000
                local period=60
                local increment=1800  # 30 minutes, 720 iterations for 15 days
            else  # 45 days
                local max_x=3888000
                local period=300
                local increment=5400  # 90 minutes, 720 iterations for 45 days
            fi
            
            # Ensure we do not exceed 1440 data points
            local n_points=$((max_x / increment))
            if [ $n_points -gt 1440 ]; then
                echo "Warning: More than 1440 data points would be retrieved. Adjusting increment."
                increment=$(( (max_x + 1439) / 1440 ))
            fi
            
            while [ $x -lt $max_x ]; do
                # Calculate window start and end times using macOS date command
                local window_end="$(date -v-${x}S '+%Y-%m-%d %H:%M:%S')"
                local window_start="$(date -v-$(($x + $increment))S '+%Y-%m-%d %H:%M:%S')"
                
                echo "Requesting CloudWatch metrics for $op:"
                echo "  Start Time: $window_start"
                echo "  End Time: $window_end"
                echo "  Period: ${period}s"
                echo "  Resolution: ${increment}s"
                
                aws cloudwatch get-metric-statistics \
                --namespace AWS/DynamoDB \
                --metric-name ${op}RequestCount \
                --start-time "$window_start" \
                --end-time "$window_end" \
                --period "$period" \
                --statistics Sum \
                --dimensions Name=TableName,Value="$TABLE_NAME" \
                --region "$REGION" \
                --query 'Datapoints[*].{Timestamp:Timestamp,RequestCount:Sum}' \
                --output json >> "$output_file"
                
                x=$((x + increment))
                local_cloudwatch_calls=$((local_cloudwatch_calls + 1))
                echo "Saved to $output_file"
            done
        done
    done
    
    # Add consumed capacity metrics calls
    local_cloudwatch_calls=$((local_cloudwatch_calls + 2))  # +2 for ConsumedReadCapacityUnits and ConsumedWriteCapacityUnits
    
    echo "Total CloudWatch API calls for table $TABLE_NAME: $local_cloudwatch_calls"
    CLOUDWATCH_CALLS_PER_TABLE=$local_cloudwatch_calls
    TOTAL_CLOUDWATCH_CALLS=$((TOTAL_CLOUDWATCH_CALLS + local_cloudwatch_calls))
}

# Process each region
for REGION in $REGIONS; do
    echo "Processing region: $REGION"
    
    # Get list of tables in the region
    TABLES=$(aws dynamodb list-tables --region "$REGION" --query "TableNames" --output text)
    
    # Skip if no tables found
    if [ -z "$TABLES" ]; then
        echo "No tables found in region $REGION"
        continue
    fi
    
    # Count total tables
    TOTAL_TABLES=$(echo "$TABLES" | wc -w)
    echo "Total tables found: $TOTAL_TABLES"
    
    # Calculate optimal number of parallel processes
    PARALLEL_PROCESSES=$(calculate_parallel_processes "$TOTAL_TABLES")
    echo "Using $PARALLEL_PROCESSES parallel processes for table processing"
    
    # Calculate initial estimated CloudWatch calls per table
    INITIAL_CLOUDWATCH_CALLS=0
    if [ "$DAYS" -eq 45 ]; then
        # 3 hours window: 9 iterations * (3 read ops + 2 write ops)
        # 15 days window: 720 iterations * (3 read ops + 2 write ops)
        # 45 days window: 720 iterations * (3 read ops + 2 write ops)
        # Plus 2 calls for consumed capacity metrics
        INITIAL_CLOUDWATCH_CALLS=$((9 * 5 + 720 * 5 + 720 * 5 + 2))
    else
        # 3 hours window: 9 iterations * (3 read ops + 2 write ops)
        # 15 days window: 720 iterations * (3 read ops + 2 write ops)
        # Plus 2 calls for consumed capacity metrics
        INITIAL_CLOUDWATCH_CALLS=$((9 * 5 + 720 * 5 + 2))
    fi
    
    # Calculate initial estimated completion time
    estimated_seconds=$((TOTAL_TABLES * INITIAL_CLOUDWATCH_CALLS * 3 / PARALLEL_PROCESSES))
    completion_time=$(date -v+${estimated_seconds}S '+%Y-%m-%d %H:%M:%S')
    echo "Initial estimated completion time: $completion_time"
    echo "Estimated CloudWatch calls per table: $INITIAL_CLOUDWATCH_CALLS"
    echo "Total estimated CloudWatch calls: $((TOTAL_TABLES * INITIAL_CLOUDWATCH_CALLS))"
    
    # Create a temporary directory for parallel processing
    TEMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TEMP_DIR"' EXIT
    
    # Process tables in parallel
    echo "$TABLES" | tr ' ' '\n' | while read -r TABLE; do
        # Apply table filters if provided (skip if -all flag is set)
        SKIP_TABLE=false
        
        if [ "$PROCESS_ALL" = false ]; then
            if [ "$USE_BOTH" = true ]; then
                # AND condition: Must match both prefix AND postfix
                if [ -n "$TABLE_PREFIX" ] && [ -n "$TABLE_POSTFIX" ]; then
                    if [[ "$TABLE" != "$TABLE_PREFIX"* || "$TABLE" != *"$TABLE_POSTFIX" ]]; then
                        echo "Skipping table $TABLE (doesn't match both prefix $TABLE_PREFIX AND postfix $TABLE_POSTFIX)"
                        SKIP_TABLE=true
                    fi
                fi
            else
                # OR condition: Check each filter independently
                if [ -n "$TABLE_PREFIX" ]; then
                    if [[ "$TABLE" != "$TABLE_PREFIX"* ]]; then
                        echo "Skipping table $TABLE (doesn't match prefix filter $TABLE_PREFIX)"
                        SKIP_TABLE=true
                    fi
                fi
                
                if [ -n "$TABLE_POSTFIX" ] && [ "$SKIP_TABLE" = false ]; then
                    if [[ "$TABLE" != *"$TABLE_POSTFIX" ]]; then
                        echo "Skipping table $TABLE (doesn't match postfix filter $TABLE_POSTFIX)"
                        SKIP_TABLE=true
                    fi
                fi
            fi
        fi
        
        # Skip this table if it didn't pass the filters
        if [ "$SKIP_TABLE" = true ]; then
            continue
        fi
        
        # Create a temporary file for this table's output
        TABLE_OUTPUT="$TEMP_DIR/${TABLE}_output.txt"
        
        # Process the table in the background
        (
            # Display banner for table processing
            echo "========================================================"
            echo "Processing DynamoDB Table: $TABLE"
            echo "AWS Account: $ACCOUNT_NUMBER"
            echo "Region: $REGION"
            echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "========================================================"
            
            # Get table description
            TABLE_INFO=$(aws dynamodb describe-table --table-name "$TABLE" --region "$REGION")
            
            echo $(echo "$TABLE_INFO"  | jq -r '.Table.TableSizeBytes') 
            # Extract table size in GB
            TABLE_SIZE_BYTES=$(echo "$TABLE_INFO" | jq -r '.Table.TableSizeBytes')
            TABLE_SIZE_GB=$(echo "scale=2; $TABLE_SIZE_BYTES / (1024*1024*1024)" | bc)
            
            # Calculate average item size
            AVG_ITEM_SIZE=$(calculate_avg_item_size "$TABLE" "$REGION")
            
            # Get provisioned capacity
            BILLING_MODE=$(echo "$TABLE_INFO" | jq -r '.Table.BillingModeSummary.BillingMode // "PROVISIONED"')
            
            if [ "$BILLING_MODE" == "PROVISIONED" ]; then
                PROV_RCU=$(echo "$TABLE_INFO" | jq -r '.Table.ProvisionedThroughput.ReadCapacityUnits')
                PROV_WCU=$(echo "$TABLE_INFO" | jq -r '.Table.ProvisionedThroughput.WriteCapacityUnits')
            else
                PROV_RCU="On-Demand"
                PROV_WCU="On-Demand"
            fi
            
            # Check for stream configuration
            STREAM_ENABLED=$(echo "$TABLE_INFO" | jq -r '.Table.StreamSpecification.StreamEnabled // "false"')
            if [ "$STREAM_ENABLED" == "true" ]; then
                STREAM_VIEW_TYPE=$(echo "$TABLE_INFO" | jq -r '.Table.StreamSpecification.StreamViewType')
            else
                STREAM_VIEW_TYPE="N/A"
            fi
            
            # Get LSI and GSI information
            LSI_COUNT=$(echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes | length // 0')
            GSI_COUNT=$(echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes | length // 0')
            
            # Initialize GSI capacity variables
            GSI_INFO=""
            
            # Extract GSI provisioned capacity if GSIs exist
            if [ "$GSI_COUNT" -gt 0 ]; then
                GSI_INFO="GSI Details: "
                GSI_INDEXES=$(echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes[]')
                
                # Loop through each GSI and extract capacity information
                echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes[] | .IndexName' | while read -r GSI_NAME; do
                    if [ "$BILLING_MODE" == "PROVISIONED" ]; then
                        GSI_RCU=$(echo "$TABLE_INFO" | jq -r ".Table.GlobalSecondaryIndexes[] | select(.IndexName == \"$GSI_NAME\") | .ProvisionedThroughput.ReadCapacityUnits")
                        GSI_WCU=$(echo "$TABLE_INFO" | jq -r ".Table.GlobalSecondaryIndexes[] | select(.IndexName == \"$GSI_NAME\") | .ProvisionedThroughput.WriteCapacityUnits")
                        GSI_INFO="$GSI_INFO [$GSI_NAME: RCU=$GSI_RCU, WCU=$GSI_WCU]"
                    else
                        GSI_INFO="$GSI_INFO [$GSI_NAME: On-Demand]"
                    fi
                done
            fi

            get_dynamodb_metrics "$TABLE" "$REGION"
            
            # Calculate and display estimated completion time for remaining tables
            local remaining_tables=$((TOTAL_TABLES - $(grep -c "," "$SUMMARY_FILE")))
            local estimated_seconds=$((remaining_tables * CLOUDWATCH_CALLS_PER_TABLE * 3))
            local completion_time=$(date -v+${estimated_seconds}S '+%Y-%m-%d %H:%M:%S')
            echo "Updated estimated completion time: $completion_time"
            
            #initialize start and end time
            START_DATE_1MONTH=$(date -v-14d '+%Y-%m-%dT%H:%M:%SZ')
            END_DATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
            
            # Get consumed read capacity units
            echo "Requesting CloudWatch metrics for ConsumedReadCapacityUnits:"
            echo "  Start Time: $START_DATE_1MONTH"
            echo "  End Time: $END_DATE"
            echo "  Period: 3600s"
            echo "  Resolution: 1 hour"

            CONSUMED_RCU=$(aws cloudwatch get-metric-statistics \
                --namespace AWS/DynamoDB \
                --metric-name ConsumedReadCapacityUnits \
                --start-time "$START_DATE_1MONTH" \
                --end-time "$END_DATE" \
                --period 3600 \
                --statistics Average \
                --dimensions Name=TableName,Value="$TABLE" \
                --region "$REGION" \
                --query 'Datapoints[*].Average' \
                --output text)
            
            # Get consumed write capacity units
            echo "Requesting CloudWatch metrics for ConsumedWriteCapacityUnits:"
            echo "  Start Time: $START_DATE_1MONTH"
            echo "  End Time: $END_DATE"
            echo "  Period: 3600s"
            echo "  Resolution: 1 hour"

            CONSUMED_WCU=$(aws cloudwatch get-metric-statistics \
                --namespace AWS/DynamoDB \
                --metric-name ConsumedWriteCapacityUnits \
                --start-time "$START_DATE_1MONTH" \
                --end-time "$END_DATE" \
                --period 3600 \
                --statistics Average \
                --dimensions Name=TableName,Value="$TABLE" \
                --region "$REGION" \
                --query 'Datapoints[*].Average' \
                --output text)
            
            # Calculate average consumed capacity
            AVG_RCU=$(echo "$CONSUMED_RCU" | awk '{ sum += $1 } END { print sum/NR }')
            AVG_WCU=$(echo "$CONSUMED_WCU" | awk '{ sum += $1 } END { print sum/NR }')
            
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
            
        ) > "$TABLE_OUTPUT" 2>&1 &
        
        # Limit the number of parallel processes
        while [ $(jobs -p | wc -l) -ge "$PARALLEL_PROCESSES" ]; do
            sleep 1
        done
    done
    
    # Wait for all background processes to complete
    wait
    
    # Combine all outputs
    cat "$TEMP_DIR"/*_output.txt
    
    # Clean up
    rm -rf "$TEMP_DIR"
done

# Print final summary
echo "Script execution summary:"
echo "Total tables processed: $TOTAL_TABLES"
echo "Total CloudWatch API calls made: $TOTAL_CLOUDWATCH_CALLS"
echo "Average CloudWatch calls per table: $((TOTAL_CLOUDWATCH_CALLS / TOTAL_TABLES))"
echo "Estimated total execution time: $((TOTAL_CLOUDWATCH_CALLS * 3)) seconds"
echo "Actual completion time: $(date '+%Y-%m-%d %H:%M:%S')"

# Count total tables processed
TOTAL_TABLES=$(grep -c "," "$SUMMARY_FILE")
TOTAL_TABLES=$((TOTAL_TABLES - 1))  # Subtract header line

echo "Analysis complete. Results saved to $OUTPUT_DIR directory."
echo "Total tables processed: $TOTAL_TABLES"
echo "Region used: $DEFAULT_REGION"

# Summary of filters applied
if [ "$PROCESS_ALL" = true ]; then
    echo "Processed ALL tables (ignoring filters)"
elif [ -n "$TABLE_PREFIX" ] || [ -n "$TABLE_POSTFIX" ]; then
    echo "Filters applied:"
    if [ -n "$TABLE_PREFIX" ]; then
        echo "- Tables starting with: '$TABLE_PREFIX'"
    fi
    if [ -n "$TABLE_POSTFIX" ]; then
        echo "- Tables ending with: '$TABLE_POSTFIX'"
    fi
    if [ "$USE_BOTH" = true ] && [ -n "$TABLE_PREFIX" ] && [ -n "$TABLE_POSTFIX" ]; then
        echo "- Required both filters to match (AND condition)"
    fi
fi

echo "Summary report: $SUMMARY_FILE"
echo "Detailed data: $DETAILED_FILE" 