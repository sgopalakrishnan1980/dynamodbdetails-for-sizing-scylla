#!/bin/bash

# Docker entrypoint script for DynamoDB Metrics Collection Tool
# This script handles AWS CLI configuration and then runs the main application

set -e

# Function to configure AWS CLI interactively
configure_aws() {
    echo "================================================"
    echo "AWS CLI Configuration"
    echo "================================================"
    
    # Check if AWS is already configured
    if [ -f "$AWS_SHARED_CREDENTIALS_FILE" ] && [ -f "$AWS_CONFIG_FILE" ]; then
        echo "AWS CLI appears to be already configured."
        echo "Files found:"
        echo "  - $AWS_SHARED_CREDENTIALS_FILE"
        echo "  - $AWS_CONFIG_FILE"
        echo ""
        read -p "Do you want to reconfigure AWS CLI? (y/N): " reconfigure
        if [[ ! "$reconfigure" =~ ^[Yy]$ ]]; then
            echo "Using existing AWS configuration."
            return 0
        fi
    fi
    
    echo "Please provide your AWS credentials:"
    echo ""
    
    # Prompt for AWS Access Key ID
    read -p "AWS Access Key ID: " aws_access_key_id
    if [ -z "$aws_access_key_id" ]; then
        echo "Error: AWS Access Key ID is required"
        exit 1
    fi
    
    # Prompt for AWS Secret Access Key (hidden input)
    read -s -p "AWS Secret Access Key: " aws_secret_access_key
    echo ""
    if [ -z "$aws_secret_access_key" ]; then
        echo "Error: AWS Secret Access Key is required"
        exit 1
    fi
    
    # Prompt for default region
    read -p "Default region (e.g., us-east-1): " default_region
    if [ -z "$default_region" ]; then
        default_region="us-east-1"
        echo "Using default region: $default_region"
    fi
    
    # Prompt for output format
    read -p "Output format (json/text/table): " output_format
    if [ -z "$output_format" ]; then
        output_format="json"
        echo "Using default output format: $output_format"
    fi
    
    echo ""
    echo "Configuring AWS CLI..."
    
    # Create AWS credentials file
    cat > "$AWS_SHARED_CREDENTIALS_FILE" << EOF
[default]
aws_access_key_id = $aws_access_key_id
aws_secret_access_key = $aws_secret_access_key
EOF
    
    # Create AWS config file
    cat > "$AWS_CONFIG_FILE" << EOF
[default]
region = $default_region
output = $output_format
EOF
    
    # Set proper permissions
    chmod 600 "$AWS_SHARED_CREDENTIALS_FILE"
    chmod 600 "$AWS_CONFIG_FILE"
    
    echo "AWS CLI configuration completed successfully!"
    echo "Configuration files created:"
    echo "  - $AWS_SHARED_CREDENTIALS_FILE"
    echo "  - $AWS_CONFIG_FILE"
    echo ""
}

# Function to test AWS credentials
test_aws_credentials() {
    echo "Testing AWS credentials..."
    if aws sts get-caller-identity > /dev/null 2>&1; then
        echo "✅ AWS credentials are valid"
        aws sts get-caller-identity
        echo ""
        return 0
    else
        echo "❌ AWS credentials are invalid or insufficient"
        echo "Please check your credentials and permissions"
        return 1
    fi
}

# Main execution
echo "================================================"
echo "DynamoDB Metrics Collection Tool"
echo "================================================"
echo ""

# Configure AWS CLI if needed
configure_aws

# Test AWS credentials
if ! test_aws_credentials; then
    echo "Exiting due to invalid AWS credentials"
    exit 1
fi

echo "Starting DynamoDB metrics collection..."
echo "================================================"
echo ""

# Execute the main application with all arguments
exec ./get_dynamodb_metrics "$@" 