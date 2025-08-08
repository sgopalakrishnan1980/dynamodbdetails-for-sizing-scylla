# DynamoDB Log Processing Flowchart

This document provides a visual representation of the processing logic used in `process_dynamodb_logs.py`.

## Main Processing Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           START PROCESSING                                 │
│                                                                             │
│  Input: Log directory path, destination, customer name, format            │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    VALIDATE INPUTS                                        │
│                                                                             │
│  ✓ Check source directory exists                                          │
│  ✓ Create destination directory if needed                                 │
│  ✓ Validate file format (csv/json)                                       │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SETUP LOGGING                                          │
│                                                                             │
│  • Create log file: customer_YYYYMMDD_HHMMSS.log                         │
│  • Configure dual output (file + console)                                │
│  • Log processing parameters                                              │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  FIND ALL REGIONS                                         │
│                                                                             │
│  • Scan directory for region folders                                      │
│  • Validate region structure (contains table subdirectories)              │
│  • Return sorted list of regions                                          │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                FOR EACH REGION                                            │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                     PROCESS REGION                                 │   │
│  │                                                                     │   │
│  │  • Log region processing start                                      │   │
│  │  • Find individual log files for region                            │   │
│  │  • Parse table_detailed.log for metadata                           │   │
│  │  • Create Excel workbook for region                                │   │
│  │  • Generate output filename with timestamp                         │   │
│  └─────────────────────┬───────────────────────────────────────────────┘   │
└─────────────────────┬───┴───────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                FIND INDIVIDUAL LOG FILES                                  │
│                                                                             │
│  Pattern: region/table/operation/metric_type/*.log                       │
│                                                                             │
│  • Scan for files in p99_latency/ and sample_count/ directories          │
│  • EXCLUDE aggregate 3hr.log and 7day.log files                         │
│  • Extract: table_name, operation, metric_type, period_type              │
│  • Group files by table name                                             │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                PARSE TABLE DETAILED LOG                                   │
│                                                                             │
│  • Look for table_detailed.log at top level or in region directory       │
│  • Parse ASCII table format                                               │
│  • Extract: TableSizeBytes, ItemCount, CreationDateTime, BillingMode     │
│  • Filter tables by region                                                │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                CREATE EXCEL WORKBOOK                                      │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                   FOR EACH TABLE                                  │   │
│  │                                                                     │   │
│  │  • Create worksheet (truncate name if >31 chars)                  │   │
│  │  • Set up headers and formatting                                   │   │
│  │  • Process individual log files                                     │   │
│  │  • Extract timestamps and values                                    │   │
│  │  • Check for missing data and log warnings                         │   │
│  │  • Generate Excel data and formatting                              │   │
│  └─────────────────────┬───────────────────────────────────────────────┘   │
└─────────────────────┬───┴───────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                PROCESS INDIVIDUAL LOG FILES                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                   FOR EACH FILE                                   │   │
│  │                                                                     │   │
│  │  • Determine file format (CSV/JSON)                                │   │
│  │  • Parse based on format:                                          │   │
│  │    - CSV: DATAPOINTS <value> <timestamp> <unit>                   │   │
│  │    - JSON: CloudWatch API response format                          │   │
│  │  • Extract (timestamp, value) pairs                               │   │
│  │  • Group by operation and metric type                              │   │
│  └─────────────────────┬───────────────────────────────────────────────┘   │
└─────────────────────┬───┴───────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                CHECK FOR MISSING DATA                                     │
│                                                                             │
│  • Compare expected operations vs. found data                           │
│  • Log warnings for missing sample count data                          │
│  • Log warnings for missing P99 latency data                           │
│  • Record in log file for troubleshooting                              │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                GENERATE EXCEL DATA                                        │
│                                                                             │
│  • Collect all unique timestamps                                         │
│  • Create timestamp headers (columns B onwards)                          │
│  • Fill Sample Count data (rows 3-9)                                    │
│  • Fill P99 Latency data (rows 12-18)                                   │
│  • Calculate and highlight max values                                    │
│  • Apply professional formatting                                         │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                CREATE SUMMARY WORKSHEET                                   │
│                                                                             │
│  • Add table metadata (size, items, billing, etc.)                      │
│  • Sort and display max sample counts                                    │
│  • Sort and display max P99 latencies                                    │
│  • Apply professional formatting and highlighting                         │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                SAVE EXCEL FILE                                            │
│                                                                             │
│  • Save workbook to destination directory                                │
│  • Log file creation success                                             │
│  • Continue to next region                                               │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    FINAL SUMMARY                                          │
│                                                                             │
│  • Log total Excel files created                                         │
│  • Log total processing time                                             │
│  • Display summary to console                                            │
│  • Save final log entry                                                  │
└─────────────────────┬───────────────────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           END PROCESSING                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Diagram

```
Input Log Directory
        │
        ▼
┌─────────────────┐
│   Region Scan   │ ──► ap-northeast-1, ap-south-1, us-east-1, etc.
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  File Discovery │ ──► region/table/operation/metric_type/*.log
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  Data Parsing   │ ──► Extract (timestamp, value) pairs
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Metadata Parse  │ ──► table_detailed.log → table metadata
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Excel Creation  │ ──► Formatted Excel files with worksheets
└─────────────────┘
        │
        ▼
┌─────────────────┐
│   Log Output    │ ──► customer_YYYYMMDD_HHMMSS.log
└─────────────────┘
```

## Key Processing Steps

### 1. Region Discovery
```
Directory Structure:
log_directory/
├── ap-northeast-1/
│   └── table_name/
│       └── operation/
│           ├── p99_latency/
│           └── sample_count/
├── us-east-1/
└── ap-south-1/
```

### 2. File Detection Logic
```
Pattern: region/*/*/*/*/*.log
Filters:
✓ Must be in p99_latency/ or sample_count/ directories
✗ Excludes aggregate 3hr.log and 7day.log files
✗ Excludes files not in expected directory structure
```

### 3. Data Parsing Logic
```
CSV Format:
DATAPOINTS <value> <timestamp> <unit>
Example: DATAPOINTS 2935.0 2025-08-05T15:29:00+00:00 Milliseconds

JSON Format:
{
  "Datapoints": [
    {
      "Timestamp": "2025-08-05T15:29:00Z",
      "SampleCount": 2935.0
    }
  ]
}
```

### 4. Excel Generation Logic
```
For each region:
├── Table Summary Worksheet
│   ├── Table metadata (size, items, billing)
│   ├── Max sample counts (sorted)
│   └── Max P99 latencies (sorted)
└── Individual Table Worksheets
    ├── Sample Count data (rows 3-9)
    ├── P99 Latency data (rows 12-18)
    ├── Max values section
    └── Professional formatting
```

## Error Handling

```
┌─────────────────┐
│   Input Error   │ ──► Validate directories and parameters
└─────────────────┘
        │
        ▼
┌─────────────────┐
│  File Not Found │ ──► Skip region, log warning, continue
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Parse Error     │ ──► Log error, skip file, continue
└─────────────────┘
        │
        ▼
┌─────────────────┐
│ Excel Error     │ ──► Log error, skip table, continue
└─────────────────┘
```

## Performance Considerations

- **Sequential Processing**: Regions processed one at a time
- **Memory Management**: Files processed individually, not loaded entirely into memory
- **Error Isolation**: Errors in one region don't affect others
- **Logging Efficiency**: Dual output (file + console) for debugging
- **File I/O Optimization**: Minimal file operations, efficient parsing

## Troubleshooting Guide

### Common Issues

1. **No regions found**
   - Check directory structure
   - Verify region folders contain table subdirectories

2. **No individual log files found**
   - Check file naming pattern
   - Verify files are in p99_latency/ or sample_count/ directories

3. **Missing data warnings**
   - Normal for tables with no activity
   - Check if files exist but are empty
   - Verify parsing logic for file format

4. **Excel creation errors**
   - Check write permissions in destination directory
   - Verify table names don't exceed Excel limits (31 chars)

### Debug Mode

Enable detailed logging by checking the log file:
```bash
tail -f customer_YYYYMMDD_HHMMSS.log
```

The log file contains:
- Processing steps and timestamps
- File discovery results
- Missing data warnings
- Error details and stack traces
- Final summary and statistics
