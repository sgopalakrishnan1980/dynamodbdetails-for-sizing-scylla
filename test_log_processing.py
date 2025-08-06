#!/usr/bin/env python3
"""
Test script to generate sample DynamoDB log files for testing the Excel processor.
This script creates sample log files with the expected format and structure.
"""

import os
import re
from datetime import datetime, timedelta
import random


def create_sample_log_file(table_name, operation, metric_type, period_type, output_dir):
    """
    Create a sample log file with the expected format.
    
    Args:
        table_name (str): Name of the table
        operation (str): Operation type (GetItem, Query, etc.)
        metric_type (str): Metric type (sample_count, p99_latency)
        period_type (str): Period type (3hr, 7day)
        output_dir (str): Output directory
    """
    # Create directory structure
    table_dir = os.path.join(output_dir, table_name)
    os.makedirs(table_dir, exist_ok=True)
    
    # Create filename
    filename = f"{table_name}_{operation}_{metric_type}-{period_type}.log"
    filepath = os.path.join(table_dir, filename)
    
    # Generate sample data
    if period_type == "3hr":
        # 3-hour data with 20-minute intervals (9 data points)
        start_time = datetime.now() - timedelta(hours=3)
        interval = timedelta(minutes=20)
        num_points = 9
    else:  # 7day
        # 7-day data with 24-hour intervals (7 data points)
        start_time = datetime.now() - timedelta(days=7)
        interval = timedelta(hours=24)
        num_points = 7
    
    # Generate sample values
    if metric_type == "sample_count":
        base_value = random.uniform(10, 100)
        values = [base_value + random.uniform(-20, 20) for _ in range(num_points)]
    else:  # p99_latency
        base_value = random.uniform(1, 10)
        values = [base_value + random.uniform(-2, 2) for _ in range(num_points)]
    
    # Write log file
    with open(filepath, 'w') as f:
        f.write("================================================\n")
        f.write(f"TABLE: {table_name}\n")
        f.write(f"OPERATION: {operation}\n")
        f.write(f"METRIC: {metric_type.replace('_', ' ').title()}\n")
        if period_type == "3hr":
            f.write("PERIOD: 3 hours (20-minute intervals)\n")
        else:
            f.write("PERIOD: 7 days (24-hour intervals)\n")
        f.write(f"GENERATED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("================================================\n")
        f.write("\n")
        
        # Add sample data
        current_time = start_time
        for i, value in enumerate(values):
            timestamp_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            f.write(f"{timestamp_str}: {value:.2f}\n")
            current_time += interval
    
    print(f"Created: {filepath}")


def main():
    """
    Create sample log files for testing.
    """
    # Test configuration
    tables = ["users", "orders", "products"]
    operations = ["GetItem", "Query", "Scan", "PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem"]
    metric_types = ["sample_count", "p99_latency"]
    period_types = ["3hr", "7day"]
    
    # Create test directory
    test_dir = "test_logs"
    os.makedirs(test_dir, exist_ok=True)
    
    print("Creating sample log files for testing...")
    
    # Create sample files for each combination
    for table in tables:
        for operation in operations:
            for metric_type in metric_types:
                for period_type in period_types:
                    create_sample_log_file(table, operation, metric_type, period_type, test_dir)
    
    print(f"\nSample log files created in: {test_dir}")
    print("\nTo test the Excel processor, run:")
    print(f"python process_dynamodb_logs.py {test_dir}")


if __name__ == "__main__":
    main()
