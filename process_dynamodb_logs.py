#!/usr/bin/env python3
"""
DynamoDB Logs to Excel Processor

This script processes DynamoDB metrics log files and creates an Excel file with
two worksheets per table - one for Sample Count and one for P99 Latency.

Usage:
    python process_dynamodb_logs.py <log_directory_path>

The Excel file will have:
- Column A: Table name and operation names (A1-A7)
- Columns B-I: 3-hour data timestamps and values (20-minute intervals)
- Columns I onwards: 7-day data timestamps and values (24-hour intervals)
"""

import os
import sys
import re
import glob
import json
from datetime import datetime
from collections import defaultdict
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils.dataframe import dataframe_to_rows


def parse_individual_log_file(file_path):
    """
    Parse an individual DynamoDB metrics log file and extract timestamps and values.
    
    Args:
        file_path (str): Path to the individual log file
        
    Returns:
        list: List of tuples (timestamp, value) extracted from the file
    """
    timestamps_values = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Parse the JSON content directly
        data = json.loads(content)
        
        # Extract datapoints from the JSON response
        if 'Datapoints' in data and isinstance(data['Datapoints'], list):
            for datapoint in data['Datapoints']:
                if 'Timestamp' not in datapoint:
                    continue
                
                timestamp_str = datapoint['Timestamp']
                value = None
                
                # Handle SampleCount (for sample_count files)
                if 'SampleCount' in datapoint and datapoint['SampleCount'] is not None:
                    value = datapoint['SampleCount']
                
                # Handle P99 latency (for p99_latency files)
                elif 'ExtendedStatistics' in datapoint and datapoint['ExtendedStatistics'] is not None:
                    if 'p99' in datapoint['ExtendedStatistics']:
                        value = datapoint['ExtendedStatistics']['p99']
                
                if value is not None:
                    # Parse timestamp (format: "2025-08-05T04:43:00Z")
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
                        timestamps_values.append((timestamp, float(value)))
                    except (ValueError, TypeError) as e:
                        continue
    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return []
    
    return timestamps_values


def find_individual_log_files(log_directory):
    """
    Find all individual DynamoDB metrics log files in the directory structure.
    
    Args:
        log_directory (str): Path to the directory containing log files
        
    Returns:
        dict: Dictionary with table names as keys and lists of file information as values
    """
    table_files = defaultdict(list)
    
    # Look for individual log files in the structure: region/table/operation/metric_type/*.log
    pattern = os.path.join(log_directory, "*", "*", "*", "*", "*.log")
    
    for file_path in glob.glob(pattern, recursive=True):
        # Extract information from the path
        path_parts = file_path.split(os.sep)
        
        # Expected structure: log_dir/region/table/operation/metric_type/filename.log
        if len(path_parts) < 6:
            continue
            
        # Extract components from path
        region = path_parts[-5]
        table_name = path_parts[-4]
        operation = path_parts[-3]
        metric_type = path_parts[-2]
        filename = path_parts[-1]
        
        # Determine period type from filename
        # Look for patterns like "20250805042954to20250805044954" (3hr) or "20250729073020to20250730073020" (7day)
        if 'to' in filename:
            # Extract the date range from filename
            date_range = filename.split('to')[0]
            
            # Determine if it's 3hr or 7day based on the date pattern
            # 3hr files have timestamps like "20250805042954" (same day)
            # 7day files have timestamps like "20250729073020" (different days)
            try:
                start_date = datetime.strptime(date_range, '%Y%m%d%H%M%S')
                end_part = filename.split('to')[1].replace('.log', '')
                end_date = datetime.strptime(end_part, '%Y%m%d%H%M%S')
                
                # Calculate time difference
                time_diff = end_date - start_date
                
                if time_diff.total_seconds() <= 4 * 3600:  # 4 hours or less
                    period_type = '3hr'
                else:
                    period_type = '7day'
                    
            except ValueError:
                # Fallback: if we can't parse, assume 3hr for recent files
                period_type = '3hr'
        else:
            period_type = '3hr'  # Default
        
        # Store file information
        table_files[table_name].append({
            'file_path': file_path,
            'operation': operation,
            'metric_type': metric_type,
            'period_type': period_type,
            'region': region
        })
    
    return table_files


def create_excel_workbook(table_files, output_file):
    """
    Create Excel workbook with data from individual log files.
    
    Args:
        table_files (dict): Dictionary with table names and their file information
        output_file (str): Path to the output Excel file
    """
    wb = Workbook()
    
    # Remove default sheet
    wb.remove(wb.active)
    
    operations = ['GetItem', 'Query', 'Scan', 'PutItem', 'UpdateItem', 'DeleteItem', 'BatchWriteItem']
    
    for table_name, files in table_files.items():
        print(f"Processing table: {table_name}")
        
        # Create two worksheets per table - one for sample count, one for p99 latency
        for metric_type in ['sample_count', 'p99_latency']:
            sheet_name = f"{table_name}_{metric_type}"
            if len(sheet_name) > 31:  # Excel sheet name limit
                sheet_name = f"{table_name[:20]}_{metric_type[:10]}"
            
            ws = wb.create_sheet(title=sheet_name)
            
            # Set up headers
            ws['A1'] = table_name
            ws['A1'].font = Font(bold=True, size=14)
            ws['A1'].fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
            
            # Add operation names in column A
            for i, operation in enumerate(operations, start=2):
                ws[f'A{i}'] = operation
                ws[f'A{i}'].font = Font(bold=True)
                ws[f'A{i}'].fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
            
            # Process files for this table and metric type
            three_hour_data = defaultdict(list)
            seven_day_data = defaultdict(list)
            
            for file_info in files:
                if file_info['metric_type'] == metric_type:
                    operation = file_info['operation']
                    period_type = file_info['period_type']
                    
                    # Parse the individual log file
                    timestamps_values = parse_individual_log_file(file_info['file_path'])
                    
                    if timestamps_values:
                        if period_type == '3hr':
                            three_hour_data[operation].extend(timestamps_values)
                        elif period_type == '7day':
                            seven_day_data[operation].extend(timestamps_values)
            
            # Sort data by timestamp for each operation
            for operation in operations:
                if operation in three_hour_data:
                    three_hour_data[operation].sort(key=lambda x: x[0])
                if operation in seven_day_data:
                    seven_day_data[operation].sort(key=lambda x: x[0])
            
            # Collect all unique timestamps from 3-hour and 7-day data
            all_timestamps = set()
            
            # Collect timestamps from 3-hour data
            for operation in operations:
                if operation in three_hour_data:
                    for timestamp, value in three_hour_data[operation]:
                        all_timestamps.add(timestamp)
            
            # Collect timestamps from 7-day data
            for operation in operations:
                if operation in seven_day_data:
                    for timestamp, value in seven_day_data[operation]:
                        all_timestamps.add(timestamp)
            
            # Sort timestamps
            sorted_timestamps = sorted(list(all_timestamps))
            
            # Create timestamp headers (columns B onwards)
            for col_idx, timestamp in enumerate(sorted_timestamps, start=2):
                # Determine if it's 3hr or 7day based on the timestamp
                # 3hr data is more recent (within last few hours), 7day is older
                now = datetime.now()
                time_diff = now - timestamp
                
                if time_diff.total_seconds() <= 4 * 3600:  # 4 hours or less
                    period_label = "3hr"
                else:
                    period_label = "7day"
                
                header_value = timestamp.strftime('%Y-%m-%d %H:%M')
                ws.cell(row=1, column=col_idx, value=header_value)
                ws.cell(row=1, column=col_idx).font = Font(bold=True)
                ws.cell(row=1, column=col_idx).fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
            
            # Add operation data as rows
            for row_idx, operation in enumerate(operations, start=2):
                ws[f'A{row_idx}'] = operation
                ws[f'A{row_idx}'].font = Font(bold=True)
                ws[f'A{row_idx}'].fill = PatternFill(start_color="E6E6E6", end_color="E6E6E6", fill_type="solid")
                
                # Create lookup dictionaries for quick access
                three_hour_lookup = {}
                if operation in three_hour_data:
                    for timestamp, value in three_hour_data[operation]:
                        three_hour_lookup[timestamp] = value
                
                seven_day_lookup = {}
                if operation in seven_day_data:
                    for timestamp, value in seven_day_data[operation]:
                        seven_day_lookup[timestamp] = value
                
                # Fill data for each timestamp column
                for col_idx, timestamp in enumerate(sorted_timestamps, start=2):
                    value = None
                    
                    # Check 3-hour data first, then 7-day data
                    if timestamp in three_hour_lookup:
                        value = three_hour_lookup[timestamp]
                    elif timestamp in seven_day_lookup:
                        value = seven_day_lookup[timestamp]
                    
                    if value is not None:
                        ws.cell(row=row_idx, column=col_idx, value=value)
            
            # Auto-adjust column widths
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
    
    # Save the workbook
    wb.save(output_file)
    print(f"Excel file created: {output_file}")


def main():
    """
    Main function to process DynamoDB log files and create Excel output.
    """
    if len(sys.argv) != 2:
        print("Usage: python process_dynamodb_logs.py <log_directory_path>")
        sys.exit(1)
    
    log_directory = sys.argv[1]
    
    if not os.path.exists(log_directory):
        print(f"Error: Directory '{log_directory}' does not exist.")
        sys.exit(1)
    
    if not os.path.isdir(log_directory):
        print(f"Error: '{log_directory}' is not a directory.")
        sys.exit(1)
    
    print(f"Scanning directory: {log_directory}")
    
    # Find all individual log files
    table_files = find_individual_log_files(log_directory)
    
    if not table_files:
        print("No individual log files found matching the expected pattern.")
        print("Expected pattern: region/table/operation/metric_type/*.log")
        sys.exit(1)
    
    print(f"Found {len(table_files)} tables with individual log files:")
    for table_name, files in table_files.items():
        print(f"  {table_name}: {len(files)} files")
    
    # Create output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"dynamodb_metrics_{timestamp}.xlsx"
    
    # Create Excel workbook
    create_excel_workbook(table_files, output_file)
    
    print(f"\nProcessing complete!")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    main()
