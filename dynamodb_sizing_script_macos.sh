#!/bin/bash -x
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
    echo "Example with filters:"
    echo "  $0 -pre users -post -prod"
    echo "Example with AWS profile:"
    echo "  $0 -all -p my-aws-profile -a 123456789012"
    exit 1
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
    
    # Process each table
    for TABLE in $TABLES; do
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
        
        echo "Analyzing table: $TABLE in region $REGION"
        
        # Get table description
        TABLE_INFO=$(aws dynamodb describe-table --table-name "$TABLE" --region "$REGION")
        
        echo $(echo "$TABLE_INFO"  | jq -r '.Table.TableSizeBytes') 
        # Extract table size in GB with 2 decimal places using bc
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

        # Function to get DynamoDB metrics with macOS-compatible date handling
        get_dynamodb_metrics() {
            # Operations to monitor
            local READ_OPS=("GetItem" "BatchGetItem" "Scan")
            local WRITE_OPS=("PutItem" "BatchWriteItem")
            
            # Time windows to analyze (in seconds)
            local TIME_WINDOWS=("10800:3hours" "1296000:15days" "3888000:45days")
            
            for window in "${TIME_WINDOWS[@]}"; do
                # Split the window string into seconds and name
                IFS=":" read -r seconds name <<< "$window"
                
                # Calculate start time based on the window (macOS compatible)
                local end_time=$(date '+%Y-%m-%dT%H:%M:%S')
                local start_time=$(date -v-${seconds}S '+%Y-%m-%dT%H:%M:%S')
                
                echo "Processing time window: $name ($start_time to $end_time)"
                
                # Process read operations
                for op in "${READ_OPS[@]}"; do
                    touch "$OUTPUT_DIR"/"${TABLE}_${op}_${name}.json"
                    output_file="./$OUTPUT_DIR"/"${TABLE}_${op}_${name}.json"
                    echo "Retrieving $op metrics for $name window..."
                    
                    endtimegranular="$(date '+%Y-%m-%d %H:%M:%S')"
                    starttimegranular="$(date -v-1440S '+%Y-%m-%d %H:%M:%S')"
                    x=1200
                    while [ $x -lt 108000 ]; do
                        local starttimegranular="$(date -v-${x}S '+%Y-%m-%d %H:%M:%S')"
                        
                        aws cloudwatch get-metric-statistics \
                        --namespace AWS/DynamoDB \
                        --metric-name ${op}RequestCount \
                        --start-time "$starttimegranular" \
                        --end-time "$endtimegranular" \
                        --period 1 \
                        --statistics Sum \
                        --dimensions Name=TableName,Value="$TABLE" \
                        --query 'Datapoints[*].{Timestamp:Timestamp,RequestCount:Sum}' \
                        --output json >> "$output_file"
                        
                        starttimegranlar="$endtimegranular"
                        echo "endtime:$endtimegranular startime:$starttimegranular counter:$x"
                        sleep 30
                        
                        x=$((x + 1200))
                        echo "Saved to $output_file"
                    done
                done
                
                # Process write operations
                for op in "${WRITE_OPS[@]}"; do
                    output_file="dynamodb_metrics/${TABLE}_${op}_${name}.json"
                    echo "Retrieving $op metrics for $name window..."
                    
                    aws cloudwatch get-metric-statistics \
                    --namespace AWS/DynamoDB \
                    --metric-name ${op}RequestCount \
                    --start-time "$start_time" \
                    --end-time "$end_time" \
                    --period 1 \
                    --statistics Sum \
                    --dimensions Name=TableName,Value="$TABLE" \
                    --query 'Datapoints[*].{Timestamp:Timestamp,RequestCount:Sum}' \
                    --output json > "$output_file"
                    
                    echo "Saved to $output_file"
                done
            done
        }
        
        get_dynamodb_metrics "$TABLE"
        
        # Initialize start and end time (macOS compatible)
        START_DATE_1MONTH=$(date -v-14d '+%Y-%m-%dT%H:%M:%SZ')
        END_DATE=$(date '+%Y-%m-%dT%H:%M:%SZ')
        
        echo "Start time set to $START_DATE_1MONTH" >> "$OUTPUT_DIR/script.log" 
        echo "End time set to $END_DATE " >> "$OUTPUT_DIR/script.log"
        
        # Get CloudWatch metrics for the last month
        # 1. Consumed RCU
        CONSUMED_RCU=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ConsumedReadCapacityUnits \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 900 \
            --statistics Maximum \
            --region "$REGION" \
            --query "Datapoints[*].Maximum" \
            --output json)
        
        CONSUMED_RCU_DETAIL=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ConsumedReadCapacityUnits \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 900 \
            --statistics Maximum \
            --region "$REGION" \
            --query "Datapoints[*].{Timestamp:Timestamp,MaxRCU:Maximum}" \
            --output json)
        
        # Dump all cloudwatch output to files 
        touch "$OUTPUT_DIR"/"$TABLE"_CONSUMED_RCU && echo "$CONSUMED_RCU_DETAIL" >> "$OUTPUT_DIR"/"$TABLE"_CONSUMED_RCU
        
        # Calculate average monthly consumed RCU with proper decimal handling
        if [ -z "$CONSUMED_RCU" ] || [ "$CONSUMED_RCU" == "[]" ]; then
            FOURTEENDAY_CONSUMED_RCU=0
        else
            # Extract maximum value with 2 decimal places
            EXTRACTED_VALUE=$(echo "$CONSUMED_RCU" | jq -r 'if length > 0 then max else 0 end')
            # Use bc for precise decimal arithmetic
            FOURTEENDAY_CONSUMED_RCU=$(echo "scale=2; $([ $EXTRACTED_VALUE -gt 0 ] && echo "$EXTRACTED_VALUE" || echo "$CONSUMED_RCU" | jq -r 'if length > 0 then add / length else 0 end')" | bc)
        fi
        
        # 2. Consumed WCU
        CONSUMED_WCU=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ConsumedWriteCapacityUnits \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 900 \
            --statistics Maximum \
            --region "$REGION" \
            --query "Datapoints[*].{Timestamp:Timestamp,MaxWCU:Maximum}" \
            --output json)
        
        touch "$OUTPUT_DIR"/"$TABLE"_CONSUMED_WCU && echo "$CONSUMED_WCU" >> "$OUTPUT_DIR"/"$TABLE"_CONSUMED_WCU
        
        # Calculate average monthly consumed WCU with proper decimal handling
        # Use bc for precise decimal arithmetic with 2 decimal places
        FOURTEENFDAY_CONSUMED_WCU=$(echo "scale=2; $([ -z $FOURTEENDAY_CONSUMED_WCU ] && echo "0" || echo "$CONSUMED_WCU" | jq -r 'add / length')" | bc)
        
        # 3. Read requests per second
        READ_OPS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=GetItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 900 \
            --statistics Maximum \
            --region "$REGION" \
            --query "Datapoints[*].SampleCount" \
            --output json)
        
        touch "$OUTPUT_DIR"/"$TABLE"_GETITEM_HISTORY && echo "$READ_OPS" >> "$OUTPUT_DIR"/"$TABLE"_GETITEM_HISTORY
        
        # Calculate average reads per second over the 14 DAYS with proper decimal handling
        # Use bc for precise decimal arithmetic with 2 decimal places
        MONTHLY_READS_PER_SEC=$(echo "scale=2; $([ -z "$READ_OPS" ] && echo "0" || [ "$READ_OPS" -eq 0 ] && echo "$READ_OPS" | jq -r 'add / length / 86400' || echo "0")" | bc)
        
        # 4. Write requests per second
        WRITE_OPS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=PutItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 900 \
            --statistics SampleCount \
            --region "$REGION" \
            --query "Datapoints[*].SampleCount" \
            --output json)
        
        touch "$OUTPUT_DIR"/"$TABLE"_PUTITEM_HISTORY && echo "$WRITE_OPS" >> "$OUTPUT_DIR"/"$TABLE"_PUTITEM_HISTORY
        
        # Calculate average writes per second over the month with proper decimal handling
        # Use bc for precise decimal arithmetic with 2 decimal places
        MONTHLY_WRITES_PER_SEC=$(echo "scale=2; $([ -z "$WRITE_OPS" ] && echo "0" || [ "$WRITE_OPS" -eq 0 ] && echo "$WRITE_OPS" | jq -r 'add / length / 86400' || echo "0")" | bc)
        
        # 5. P99 Read Latency
        READ_P99_LATENCY=$(echo "scale=2; $(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=GetItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 20160 \
            --extended-statistics p99 \
            --region "$REGION" \
            --query "Datapoints[0].p99" \
            --output text)" | bc)
        
        touch "$OUTPUT_DIR"/"$TABLE"_GETITEM_READP99_HISTORY && echo "$READ_P99_LATENCY" >> "$OUTPUT_DIR"/"$TABLE"_GETITEM_READP99_HISTORY
        
        # 6. P99 Write Latency
        WRITE_P99_LATENCY=$(echo "scale=2; $(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=PutItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 20160 \
            --extended-statistics p99 \
            --region "$REGION" \
            --query "Datapoints[0].p99" \
            --output text)" | bc)
        
        touch "$OUTPUT_DIR"/"$TABLE"_PUTITEM_WRITEP99_HISTORY && echo "$WRITE_P99_LATENCY" >> "$OUTPUT_DIR"/"$TABLE"_PUTITEM_WRITEP99_HISTORY
        
        # Sum of all successful requests (macOS compatible date handling)
        SUMREQUESTS14DAYS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time $(date -v-14d '+%Y-%m-%dT%H:%M:%SZ') \
            --end-time $(date '+%Y-%m-%dT%H:%M:%SZ') \
            --period 1209600 \
            --statistics SampleCount \
            --region $REGION)
        
        SUMREQUESTS7DAYS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time $(date -v-7d '+%Y-%m-%dT%H:%M:%SZ') \
            --end-time $(date '+%Y-%m-%dT%H:%M:%SZ') \
            --period 604800 \
            --statistics SampleCount \
            --region $REGION)
        
        SUMREQUESTS1DAYS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time $(date -v-1d '+%Y-%m-%dT%H:%M:%SZ') \
            --end-time $(date '+%Y-%m-%dT%H:%M:%SZ') \
            --period 86400 \
            --statistics SampleCount \
            --region $REGION)
        
        SUMREQUESTS12HOURS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time $(date -v-12H '+%Y-%m-%dT%H:%M:%SZ') \
            --end-time $(date '+%Y-%m-%dT%H:%M:%SZ') \
            --period 43200 \
            --statistics SampleCount \
            --region $REGION)
        
        SUMREQUESTS1HOURS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time $(date -v-1H '+%Y-%m-%dT%H:%M:%SZ') \
            --end-time $(date '+%Y-%m-%dT%H:%M:%SZ') \
            --period 3600 \
            --statistics SampleCount \
            --region $REGION)
        
        # Handle null or missing values
        [ "$READ_P99_LATENCY" == "None" ] && READ_P99_LATENCY="N/A"
        [ "$WRITE_P99_LATENCY" == "None" ] && WRITE_P99_LATENCY="N/A"
        
        # Add to summary CSV
        echo "$TABLE,$AVG_ITEM_SIZE,$TABLE_SIZE_GB,$PROV_RCU,$PROV_WCU,$SUMREQUESTS1HOURS,$SUMREQUESTS12HOURS,$SUMREQUESTS1DAYS,$SUMREQUESTS7DAYS,$SUMREQUESTS14DAYS,$FOURTEENDAY_CONSUMED_RCU,$FOURTEENFDAY_CONSUMED_WCU,$MONTHLY_READS_PER_SEC,$MONTHLY_WRITES_PER_SEC,$READ_P99_LATENCY,$WRITE_P99_LATENCY,$STREAM_ENABLED,$STREAM_VIEW_TYPE,$LSI_COUNT,$GSI_COUNT" >> "$SUMMARY_FILE"
        
        # Store detailed info in JSON
        TABLE_DETAILED=$(cat <<EOF
{
  "tableName": "$TABLE",
  "region": "$REGION",
  "avgItemSizeKB": $AVG_ITEM_SIZE,
  "tableSizeGB": $TABLE_SIZE_GB,
  "provisionedRCU": "$PROV_RCU",
  "provisionedWCU": "$PROV_WCU",
  "sumrequests1hour": "$SUMREQUESTS1HOURS",
  "sumrequests12hour": "$SUMREQUESTS12HOURS",
  "sumrequests1day": "$SUMREQUESTS1DAYS",
  "sumrequests7days": "$SUMREQUESTS7DAYS",
  "sumrequests14days": "$SUMREQUESTS14DAYS",
  "monthlyConsumedRCU": $FOURTEENDAY_CONSUMED_RCU,
  "monthlyConsumedWCU": $FOURTEENFDAY_CONSUMED_WCU,
  "monthlyReadsPerSec": $MONTHLY_READS_PER_SEC,
  "monthlyWritesPerSec": $MONTHLY_WRITES_PER_SEC,
  "readP99LatencyMS": "$READ_P99_LATENCY",
  "writeP99LatencyMS": "$WRITE_P99_LATENCY",
  "streamEnabled": $STREAM_ENABLED,
  "streamViewType": "$STREAM_VIEW_TYPE",
  "lsiCount": $LSI_COUNT,
  "gsiCount": $GSI_COUNT,
  "account": "$ACCOUNT_NUMBER"
EOF
)
        
        # Add GSI details if available
        if [ -n "$GSI_INFO" ]; then
            TABLE_DETAILED="${TABLE_DETAILED},
  \"gsiDetails\": \"$GSI_INFO\""
        fi
        
        # Close the JSON object
        TABLE_DETAILED="${TABLE_DETAILED}
}"
        
        echo "$TABLE_DETAILED" >> "$DETAILED_FILE"
        
        # Create a separate file for each table with GSI details if GSIs exist
        if [ "$GSI_COUNT" -gt 0 ]; then
            GSI_FILE="$OUTPUT_DIR/${TABLE}_gsi_details.json"
            echo "{" > "$GSI_FILE"
            echo "  \"tableName\": \"$TABLE\"," >> "$GSI_FILE"
            echo "  \"gsiDetails\": [" >> "$GSI_FILE"
            
            # Extract GSI details with jq
            GSI_INDEX=0
            echo "$TABLE_INFO" | jq -r '.Table.GlobalSecondaryIndexes[] | .IndexName' | while read -r GSI_NAME; do
                if [ "$GSI_INDEX" -gt 0 ]; then
                    echo "," >> "$GSI_FILE"
                fi
                
                GSI_DETAILS=$(echo "$TABLE_INFO" | jq -r ".Table.GlobalSecondaryIndexes[] | select(.IndexName == \"$GSI_NAME\")")
                
                echo "    {" >> "$GSI_FILE"
                echo "      \"indexName\": \"$GSI_NAME\"," >> "$GSI_FILE"
                
                # Extract key schema
                GSI_KEY_SCHEMA=$(echo "$GSI_DETAILS" | jq -r '.KeySchema')
                echo "      \"keySchema\": $GSI_KEY_SCHEMA," >> "$GSI_FILE"
                
                # Extract projection
                GSI_PROJECTION=$(echo "$GSI_DETAILS" | jq -r '.Projection')
                echo "      \"projection\": $GSI_PROJECTION," >> "$GSI_FILE"
                
                # Extract provisioned throughput if applicable
                if [ "$BILLING_MODE" == "PROVISIONED" ]; then
                    GSI_RCU=$(echo "$GSI_DETAILS" | jq -r '.ProvisionedThroughput.ReadCapacityUnits')
                    GSI_WCU=$(echo "$GSI_DETAILS" | jq -r '.ProvisionedThroughput.WriteCapacityUnits')
                    echo "      \"provisionedThroughput\": {" >> "$GSI_FILE"
                    echo "        \"readCapacityUnits\": $GSI_RCU," >> "$GSI_FILE"
                    echo "        \"writeCapacityUnits\": $GSI_WCU" >> "$GSI_FILE"
                    echo "      }" >> "$GSI_FILE"
                else
                    echo "      \"billingMode\": \"On-Demand\"" >> "$GSI_FILE"
                fi
                
                echo "    }" >> "$GSI_FILE"
                GSI_INDEX=$((GSI_INDEX + 1))
            done
            
            echo "  ]" >> "$GSI_FILE"
            echo "}" >> "$GSI_FILE"
        fi
        
        # Create a separate file for each table with LSI details if LSIs exist
        if [ "$LSI_COUNT" -gt 0 ]; then
            LSI_FILE="$OUTPUT_DIR/${TABLE}_lsi_details.json"
            echo "{" > "$LSI_FILE"
            echo "  \"tableName\": \"$TABLE\"," >> "$LSI_FILE"
            echo "  \"lsiDetails\": [" >> "$LSI_FILE"
            
            # Extract LSI details with jq
            LSI_INDEX=0
            echo "$TABLE_INFO" | jq -r '.Table.LocalSecondaryIndexes[] | .IndexName' | while read -r LSI_NAME; do
                if [ "$LSI_INDEX" -gt 0 ]; then
                    echo "," >> "$LSI_FILE"
                fi
                
                LSI_DETAILS=$(echo "$TABLE_INFO" | jq -r ".Table.LocalSecondaryIndexes[] | select(.IndexName == \"$LSI_NAME\")")
                
                echo "    {" >> "$LSI_FILE"
                echo "      \"indexName\": \"$LSI_NAME\"," >> "$LSI_FILE"
                
                # Extract key schema
                LSI_KEY_SCHEMA=$(echo "$LSI_DETAILS" | jq -r '.KeySchema')
                echo "      \"keySchema\": $LSI_KEY_SCHEMA," >> "$LSI_FILE"
                
                # Extract projection
                LSI_PROJECTION=$(echo "$LSI_DETAILS" | jq -r '.Projection')
                echo "      \"projection\": $LSI_PROJECTION" >> "$LSI_FILE"
                
                echo "    }" >> "$LSI_FILE"
                LSI_INDEX=$((LSI_INDEX + 1))
            done
            
            echo "  ]" >> "$LSI_FILE"
            echo "}" >> "$LSI_FILE"
        fi
    done
done

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