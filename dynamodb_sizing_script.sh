#!/bin/bash -x
# DynamoDB and ScyllaDB Sizing Collection Script
# This script collects sizing and usage metrics for DynamoDB tables

set -e

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
echo "Table Name,Avg Item Size (KB),Total Size (GB),Provisioned RCU,Provisioned WCU,Monthly Consumed RCU (Avg),Monthly Consumed WCU (Avg),Monthly Reads/Sec (Avg),Monthly Writes/Sec (Avg),Read P99 Latency (ms),Write P99 Latency (ms),Streams Enabled,Stream View Type,LSI Count,GSI Count" >> "$OUTPUT_DIR/dynamodb_summary.csv"

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
    
    # Calculate average item size in KB
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
        
	#initialize start and end time 
	START_DATE_1MONTH=$(date -u -d "1 month ago" +"%Y-%m-%dT%H:%M:%SZ")
	END_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        # Get CloudWatch metrics for the last month
        # 1. Consumed RCU
        CONSUMED_RCU=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ConsumedReadCapacityUnits \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 86400 \
            --statistics Average \
            --region "$REGION" \
            --query "Datapoints[*].Average" \
            --output json)
        
        # Calculate average monthly consumed RCU
        MONTHLY_CONSUMED_RCU=$( [-z $MONTHLY_CONSUMED_RCU ] && echo "Monthly Consumed RCU is null"   || echo "$CONSUMED_RCU" | jq -r 'add / length')
        
        # 2. Consumed WCU
        CONSUMED_WCU=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name ConsumedWriteCapacityUnits \
            --dimensions Name=TableName,Value="$TABLE" \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 86400 \
            --statistics Average \
            --region "$REGION" \
            --query "Datapoints[*].Average" \
            --output json)
        
        # Calculate average monthly consumed WCU
        MONTHLY_CONSUMED_WCU=$( [ -z $MONTHLY_CONSUMED_WCU ] && echo "Monthly Consumed WCU is null " || echo "$CONSUMED_WCU" | jq -r 'add / length')
        
        # 3. Read requests per second
        READ_OPS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=GetItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 86400 \
            --statistics SampleCount \
            --region "$REGION" \
            --query "Datapoints[*].SampleCount" \
            --output json)
        
        # Calculate average reads per second over the month
        MONTHLY_READS_PER_SEC=$( [-z $READ_OPS] && echo "READs per second is null " ||  [ $READ_OPS -eq 0 ] && echo "$READ_OPS" | jq -r 'add / length / 86400' || echo "0" )
        
        # 4. Write requests per second
        WRITE_OPS=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=PutItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 86400 \
            --statistics SampleCount \
            --region "$REGION" \
            --query "Datapoints[*].SampleCount" \
            --output json)
        
        # Calculate average writes per second over the month
	MONTHLY_WRITES_PER_SEC=$( [-z $WRITE_OPS ]  && echo "0" || [$WRITE_OPS -eq 0 ] && echo "$WRITE_OPS" | jq -r 'add / length / 86400'|| echo "0" )
        
        # 5. P99 Read Latency
        READ_P99_LATENCY=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=GetItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 2592000 \
            --extended-statistics p99 \
            --region "$REGION" \
            --query "Datapoints[0].p99" \
            --output text)
        
        # 6. P99 Write Latency
        WRITE_P99_LATENCY=$(aws cloudwatch get-metric-statistics \
            --namespace AWS/DynamoDB \
            --metric-name SuccessfulRequestLatency \
            --dimensions Name=TableName,Value="$TABLE" Name=Operation,Value=PutItem \
            --start-time "$START_DATE_1MONTH" \
            --end-time "$END_DATE" \
            --period 2592000 \
            --extended-statistics p99 \
            --region "$REGION" \
            --query "Datapoints[0].p99" \
            --output text)
        
        # Handle null or missing values
        [ "$READ_P99_LATENCY" == "None" ] && READ_P99_LATENCY="N/A"
        [ "$WRITE_P99_LATENCY" == "None" ] && WRITE_P99_LATENCY="N/A"
        
        # Add to summary CSV
        echo "$TABLE,$AVG_ITEM_SIZE,$TABLE_SIZE_GB,$PROV_RCU,$PROV_WCU,$MONTHLY_CONSUMED_RCU,$MONTHLY_CONSUMED_WCU,$MONTHLY_READS_PER_SEC,$MONTHLY_WRITES_PER_SEC,$READ_P99_LATENCY,$WRITE_P99_LATENCY,$STREAM_ENABLED,$STREAM_VIEW_TYPE,$LSI_COUNT,$GSI_COUNT" >> "$SUMMARY_FILE"
        
        # Store detailed info in JSON
        TABLE_DETAILED=$(cat <<EOF
{
  "tableName": "$TABLE",
  "region": "$REGION",
  "avgItemSizeKB": $AVG_ITEM_SIZE,
  "tableSizeGB": $TABLE_SIZE_GB,
  "provisionedRCU": "$PROV_RCU",
  "provisionedWCU": "$PROV_WCU",
  "monthlyConsumedRCU": $MONTHLY_CONSUMED_RCU,
  "monthlyConsumedWCU": $MONTHLY_CONSUMED_WCU,
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
