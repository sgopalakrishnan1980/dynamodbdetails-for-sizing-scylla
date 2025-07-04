package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/spf13/cobra"
)

// Global variables
var (
	logDir             string
	awsProfile         string
	period             int64 = 1
	regionsToProbe     []string
	tableNames         []string
	useInstanceProfile bool
	totalAWSCalls      int64
	waitAWSCalls       int64
	waitThreshold      int64 = 1000
)

// Operation types
var readOperations = []string{"GetItem", "Query", "Scan"}
var writeOperations = []string{"PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem"}

// Logger interface
type Logger struct {
	file *os.File
}

func NewLogger(logDir string) (*Logger, error) {
	timestamp := time.Now().Format("20060102_150405")
	logFile := filepath.Join(logDir, fmt.Sprintf("script_execution_%s.log", timestamp))

	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %v", err)
	}

	return &Logger{file: file}, nil
}

func (l *Logger) Log(level, message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logMessage := fmt.Sprintf("[%s] [%s] %s\n", timestamp, level, message)

	// Write to file
	l.file.WriteString(logMessage)

	// Also print to console
	fmt.Print(logMessage)
}

func (l *Logger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

// AWS client wrapper
type AWSClient struct {
	cloudwatch *cloudwatch.Client
	dynamodb   *dynamodb.Client
	sts        *sts.Client
	region     string
	logger     *Logger
}

func NewAWSClient(ctx context.Context, region, profile string, useInstanceProfile bool, logger *Logger) (*AWSClient, error) {
	var cfg aws.Config
	var err error

	if useInstanceProfile {
		// Use instance profile
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, fmt.Errorf("failed to load default config: %v", err)
		}
	} else if profile != "" {
		// Use specific profile
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(profile),
			config.WithRegion(region))
		if err != nil {
			return nil, fmt.Errorf("failed to load config with profile %s: %v", profile, err)
		}
	} else {
		// Use default config
		cfg, err = config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, fmt.Errorf("failed to load default config: %v", err)
		}
	}

	// Verify credentials
	stsClient := sts.NewFromConfig(cfg)
	_, err = stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to verify AWS credentials: %v", err)
	}

	return &AWSClient{
		cloudwatch: cloudwatch.NewFromConfig(cfg),
		dynamodb:   dynamodb.NewFromConfig(cfg),
		sts:        stsClient,
		region:     region,
		logger:     logger,
	}, nil
}

// Get list of tables
func (c *AWSClient) ListTables(ctx context.Context) ([]string, error) {
	var tables []string
	var lastEvaluatedTableName *string

	for {
		input := &dynamodb.ListTablesInput{}
		if lastEvaluatedTableName != nil {
			input.ExclusiveStartTableName = lastEvaluatedTableName
		}

		result, err := c.dynamodb.ListTables(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list tables: %v", err)
		}

		tables = append(tables, result.TableNames...)

		if result.LastEvaluatedTableName == nil {
			break
		}
		lastEvaluatedTableName = result.LastEvaluatedTableName
	}

	return tables, nil
}

// Get table details
func (c *AWSClient) DescribeTable(ctx context.Context, tableName string) (*dynamodb.DescribeTableOutput, error) {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	return c.dynamodb.DescribeTable(ctx, input)
}

// Get CloudWatch metrics
func (c *AWSClient) GetMetricStatistics(ctx context.Context, tableName, operation, metricName string, startTime, endTime time.Time, period int64) (*cloudwatch.GetMetricStatisticsOutput, error) {
	// Always use <Operation>Latency for all operations
	finalMetricName := operation + "Latency"

	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/DynamoDB"),
		MetricName: aws.String(finalMetricName),
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Period:     aws.Int32(int32(period)),
		Dimensions: []types.Dimension{
			{
				Name:  aws.String("TableName"),
				Value: aws.String(tableName),
			},
			{
				Name:  aws.String("Operation"),
				Value: aws.String(operation),
			},
		},
	}

	// Add statistics based on metric type
	if metricName == "SampleCount" {
		input.Statistics = []types.Statistic{types.StatisticSampleCount}
	} else if metricName == "P99" {
		input.ExtendedStatistics = []string{"p99"}
	}

	return c.cloudwatch.GetMetricStatistics(ctx, input)
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Metrics collector
type MetricsCollector struct {
	client *AWSClient
	logger *Logger
	logDir string
}

func NewMetricsCollector(client *AWSClient, logger *Logger, logDir string) *MetricsCollector {
	return &MetricsCollector{
		client: client,
		logger: logger,
		logDir: logDir,
	}
}

// Collect sample counts
func (mc *MetricsCollector) CollectSampleCounts(ctx context.Context, tableName string, startTime, endTime time.Time, iteration int64, period int64) error {
	// Process read operations
	for _, op := range readOperations {
		if err := mc.collectMetric(ctx, tableName, op, "SampleCount", startTime, endTime, iteration, period); err != nil {
			return fmt.Errorf("failed to collect sample count for %s: %v", op, err)
		}
	}

	// Process write operations
	for _, op := range writeOperations {
		if err := mc.collectMetric(ctx, tableName, op, "SampleCount", startTime, endTime, iteration, period); err != nil {
			return fmt.Errorf("failed to collect sample count for %s: %v", op, err)
		}
	}

	return nil
}

// Collect P99 latency
func (mc *MetricsCollector) CollectP99Latency(ctx context.Context, tableName string, startTime, endTime time.Time, iteration int64, period int64) error {
	// Process read operations
	for _, op := range readOperations {
		if err := mc.collectMetric(ctx, tableName, op, "P99", startTime, endTime, iteration, period); err != nil {
			return fmt.Errorf("failed to collect P99 latency for %s: %v", op, err)
		}
	}

	// Process write operations
	for _, op := range writeOperations {
		if err := mc.collectMetric(ctx, tableName, op, "P99", startTime, endTime, iteration, period); err != nil {
			return fmt.Errorf("failed to collect P99 latency for %s: %v", op, err)
		}
	}

	return nil
}

// Collect a single metric
func (mc *MetricsCollector) collectMetric(ctx context.Context, tableName, operation, metricType string, startTime, endTime time.Time, iteration, period int64) error {
	// Increment counters
	totalAWSCalls++
	waitAWSCalls++

	mc.logger.Log("DEBUG", fmt.Sprintf("AWS call counters incremented - Total: %d, Wait: %d", totalAWSCalls, waitAWSCalls))

	// Create directory structure
	regionDir := filepath.Join(mc.logDir, mc.client.region)
	tableDir := filepath.Join(regionDir, tableName)
	opDir := filepath.Join(tableDir, operation)

	var metricDir string
	if metricType == "SampleCount" {
		metricDir = filepath.Join(opDir, "sample_count")
	} else {
		metricDir = filepath.Join(opDir, "p99_latency")
	}

	if err := os.MkdirAll(metricDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", metricDir, err)
	}

	// Get metrics
	result, err := mc.client.GetMetricStatistics(ctx, tableName, operation, metricType, startTime, endTime, period)
	if err != nil {
		return fmt.Errorf("failed to get metric statistics: %v", err)
	}

	// Create log file
	startTimeStr := startTime.Format("20060102150405")
	endTimeStr := endTime.Format("20060102150405")

	var logFileName string
	if metricType == "SampleCount" {
		logFileName = fmt.Sprintf("%s_SampleCount_%sto%s.log", operation, startTimeStr, endTimeStr)
	} else {
		logFileName = fmt.Sprintf("p99_%s_%sto%s.log", operation, startTimeStr, endTimeStr)
	}

	logFilePath := filepath.Join(metricDir, logFileName)

	// Write results to file
	file, err := os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to create log file %s: %v", logFilePath, err)
	}
	defer file.Close()

	// Convert result to JSON for logging
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result to JSON: %v", err)
	}

	if _, err := file.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write to log file: %v", err)
	}

	mc.logger.Log("INFO", fmt.Sprintf("Created metrics log file: %s", logFilePath))

	return nil
}

// Consolidate logs
func (mc *MetricsCollector) ConsolidateLogs(periodType string) error {
	mc.logger.Log("INFO", fmt.Sprintf("Starting log consolidation for %s period...", periodType))

	// Process each region directory
	regionDirs, err := filepath.Glob(filepath.Join(mc.logDir, "*"))
	if err != nil {
		return fmt.Errorf("failed to glob region directories: %v", err)
	}

	for _, regionDir := range regionDirs {
		regionInfo, err := os.Stat(regionDir)
		if err != nil || !regionInfo.IsDir() {
			continue
		}

		regionName := filepath.Base(regionDir)
		mc.logger.Log("INFO", fmt.Sprintf("Processing region: %s", regionName))

		// Process each table directory
		tableDirs, err := filepath.Glob(filepath.Join(regionDir, "*"))
		if err != nil {
			continue
		}

		for _, tableDir := range tableDirs {
			tableInfo, err := os.Stat(tableDir)
			if err != nil || !tableInfo.IsDir() {
				continue
			}

			tableName := filepath.Base(tableDir)
			mc.logger.Log("INFO", fmt.Sprintf("Processing table: %s in region: %s", tableName, regionName))

			// Process each operation
			allOperations := append(readOperations, writeOperations...)
			for _, op := range allOperations {
				opDir := filepath.Join(tableDir, op)

				// Process sample_count metrics
				sampleCountDir := filepath.Join(opDir, "sample_count")
				if err := mc.consolidateMetricFiles(sampleCountDir, tableDir, tableName, op, "sample_count", periodType); err != nil {
					mc.logger.Log("ERROR", fmt.Sprintf("Failed to consolidate sample count for %s: %v", op, err))
				}

				// Process p99_latency metrics
				p99LatencyDir := filepath.Join(opDir, "p99_latency")
				if err := mc.consolidateMetricFiles(p99LatencyDir, tableDir, tableName, op, "p99_latency", periodType); err != nil {
					mc.logger.Log("ERROR", fmt.Sprintf("Failed to consolidate P99 latency for %s: %v", op, err))
				}
			}
		}
	}

	mc.logger.Log("INFO", fmt.Sprintf("Log consolidation completed for %s period", periodType))
	return nil
}

// Consolidate metric files
func (mc *MetricsCollector) consolidateMetricFiles(metricDir, tableDir, tableName, operation, metricType, periodType string) error {
	// Check if directory exists
	if _, err := os.Stat(metricDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, skip
	}

	// Find all log files
	logFiles, err := filepath.Glob(filepath.Join(metricDir, "*.log"))
	if err != nil {
		return fmt.Errorf("failed to glob log files: %v", err)
	}

	if len(logFiles) == 0 {
		return nil // No files to consolidate
	}

	// Sort files
	sort.Strings(logFiles)

	// Create consolidated file
	consolidatedFileName := fmt.Sprintf("%s_%s_%s-%s.log", tableName, operation, metricType, periodType)
	consolidatedFilePath := filepath.Join(tableDir, consolidatedFileName)

	file, err := os.Create(consolidatedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create consolidated file: %v", err)
	}
	defer file.Close()

	// Write header
	header := fmt.Sprintf(`================================================
TABLE: %s
OPERATION: %s
METRIC: %s
PERIOD: %s
GENERATED: %s
================================================

`, tableName, operation, strings.Title(strings.Replace(metricType, "_", " ", -1)),
		getPeriodDescription(periodType), time.Now().Format("2006-01-02 15:04:05"))

	if _, err := file.WriteString(header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Concatenate all files
	for _, logFile := range logFiles {
		fileName := filepath.Base(logFile)
		separator := fmt.Sprintf("--- %s ---\n", fileName)

		if _, err := file.WriteString(separator); err != nil {
			return fmt.Errorf("failed to write separator: %v", err)
		}

		content, err := os.ReadFile(logFile)
		if err != nil {
			return fmt.Errorf("failed to read log file %s: %v", logFile, err)
		}

		if _, err := file.Write(content); err != nil {
			return fmt.Errorf("failed to write content: %v", err)
		}

		if _, err := file.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
	}

	mc.logger.Log("INFO", fmt.Sprintf("Created consolidated %s log: %s", metricType, consolidatedFilePath))
	return nil
}

// Get period description
func getPeriodDescription(periodType string) string {
	if periodType == "3hr" {
		return "3 hours (20-minute intervals)"
	}
	return "7 days (24-hour intervals)"
}

// Main execution function
func runMetricsCollection(cmd *cobra.Command, args []string) error {
	// Create log directory
	timestamp := time.Now().Format("010206150405")
	logDir = fmt.Sprintf("dynamo_metrics_logs_%s", timestamp)

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	// Initialize logger
	logger, err := NewLogger(logDir)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %v", err)
	}
	defer logger.Close()

	logger.Log("INFO", "Starting DynamoDB metrics collection")
	logger.Log("DEBUG", fmt.Sprintf("Initial AWS call counter: %d", totalAWSCalls))
	logger.Log("DEBUG", fmt.Sprintf("Initial wait AWS call counter: %d", waitAWSCalls))

	// Determine regions to process
	if len(regionsToProbe) == 0 {
		// Use default region
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return fmt.Errorf("failed to load default config: %v", err)
		}
		regionsToProbe = []string{cfg.Region}
		logger.Log("INFO", fmt.Sprintf("Using default region: %s", cfg.Region))
	} else {
		logger.Log("INFO", fmt.Sprintf("Using specified regions: %v", regionsToProbe))
	}

	// Process each region
	var allTables []string
	for _, region := range regionsToProbe {
		logger.Log("INFO", fmt.Sprintf("Processing region: %s", region))

		// Create AWS client
		client, err := NewAWSClient(context.Background(), region, awsProfile, useInstanceProfile, logger)
		if err != nil {
			return fmt.Errorf("failed to create AWS client for region %s: %v", region, err)
		}

		// Get list of tables
		tables, err := client.ListTables(context.Background())
		if err != nil {
			return fmt.Errorf("failed to list tables in region %s: %v", region, err)
		}

		if len(tables) == 0 {
			logger.Log("INFO", fmt.Sprintf("No tables found in region %s", region))
			continue
		}

		logger.Log("DEBUG", fmt.Sprintf("Found tables in region %s: %v", region, tables))

		// Filter tables if specified
		if len(tableNames) > 0 {
			var filteredTables []string
			for _, table := range tables {
				for _, targetTable := range tableNames {
					if table == targetTable {
						filteredTables = append(filteredTables, table)
						break
					}
				}
			}
			tables = filteredTables
		}

		// Add region info to table names
		for _, table := range tables {
			allTables = append(allTables, fmt.Sprintf("%s:%s", table, region))
		}
	}

	if len(allTables) == 0 {
		return fmt.Errorf("no tables found to process")
	}

	logger.Log("INFO", fmt.Sprintf("Found %d tables to process", len(allTables)))

	// Create metrics collector
	collector := NewMetricsCollector(nil, logger, logDir)

	// 3-hour collection (9 iterations, 20-minute intervals)
	logger.Log("INFO", "Starting DynamoDB metrics collection for last 3 hours in 20-minute intervals...")

	currentTime := time.Now().UTC()
	startTime := currentTime.Add(-20 * time.Minute)

	for i := int64(1); i <= 9; i++ {
		logger.Log("INFO", fmt.Sprintf("Starting iteration %d of 9", i))
		logger.Log("INFO", fmt.Sprintf("Time range: %s to %s", startTime.Format(time.RFC3339), currentTime.Format(time.RFC3339)))

		for _, tableInfo := range allTables {
			parts := strings.Split(tableInfo, ":")
			if len(parts) != 2 {
				continue
			}

			tableName := parts[0]
			region := parts[1]

			// Create client for this region
			client, err := NewAWSClient(context.Background(), region, awsProfile, useInstanceProfile, logger)
			if err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to create AWS client for table %s: %v", tableName, err))
				continue
			}

			collector.client = client

			// Collect sample counts
			if err := collector.CollectSampleCounts(context.Background(), tableName, startTime, currentTime, i, period); err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to collect sample counts for table %s: %v", tableName, err))
			}

			// Collect P99 latency
			if err := collector.CollectP99Latency(context.Background(), tableName, startTime, currentTime, i, period); err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to collect P99 latency for table %s: %v", tableName, err))
			}

			// Check wait threshold
			if waitAWSCalls >= waitThreshold {
				logger.Log("INFO", fmt.Sprintf("Wait counter reached %d, waiting for background processes...", waitAWSCalls))
				// In Go, we don't need to wait since we're not using goroutines for AWS calls
				logger.Log("INFO", "Background processes completed, resetting counter...")
				waitAWSCalls = 0
			}

			logger.Log("INFO", fmt.Sprintf("Completed iteration %d for table %s", i, tableName))
		}

		// Update times for next iteration
		currentTime = startTime
		startTime = startTime.Add(-20 * time.Minute)
	}

	// Consolidate 3-hour logs
	if err := collector.ConsolidateLogs("3hr"); err != nil {
		logger.Log("ERROR", fmt.Sprintf("Failed to consolidate 3-hour logs: %v", err))
	}

	// 7-day collection (7 iterations, 24-hour intervals)
	logger.Log("INFO", "Starting DynamoDB metrics collection for last 7 days in 24-hour intervals...")

	currentTime = time.Now().UTC()
	startTime = currentTime.Add(-24 * time.Hour)

	for i := int64(1); i <= 7; i++ {
		logger.Log("INFO", fmt.Sprintf("Starting 7-day iteration %d of 7", i))
		logger.Log("INFO", fmt.Sprintf("Time range: %s to %s", startTime.Format(time.RFC3339), currentTime.Format(time.RFC3339)))

		for _, tableInfo := range allTables {
			parts := strings.Split(tableInfo, ":")
			if len(parts) != 2 {
				continue
			}

			tableName := parts[0]
			region := parts[1]

			// Create client for this region
			client, err := NewAWSClient(context.Background(), region, awsProfile, useInstanceProfile, logger)
			if err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to create AWS client for table %s: %v", tableName, err))
				continue
			}

			collector.client = client

			// Collect sample counts (60-second period)
			if err := collector.CollectSampleCounts(context.Background(), tableName, startTime, currentTime, i, 60); err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to collect sample counts for table %s: %v", tableName, err))
			}

			// Collect P99 latency (60-second period)
			if err := collector.CollectP99Latency(context.Background(), tableName, startTime, currentTime, i, 60); err != nil {
				logger.Log("ERROR", fmt.Sprintf("Failed to collect P99 latency for table %s: %v", tableName, err))
			}

			// Check wait threshold
			if waitAWSCalls >= waitThreshold {
				logger.Log("INFO", fmt.Sprintf("Wait counter reached %d, waiting for background processes...", waitAWSCalls))
				logger.Log("INFO", "Background processes completed, resetting counter...")
				waitAWSCalls = 0
			}

			logger.Log("INFO", fmt.Sprintf("Completed 7-day iteration %d for table %s", i, tableName))
		}

		// Update times for next iteration
		currentTime = startTime
		startTime = startTime.Add(-24 * time.Hour)
	}

	// Consolidate 7-day logs
	if err := collector.ConsolidateLogs("7day"); err != nil {
		logger.Log("ERROR", fmt.Sprintf("Failed to consolidate 7-day logs: %v", err))
	}

	// Final wait check
	if waitAWSCalls >= waitThreshold {
		logger.Log("INFO", fmt.Sprintf("Final wait: Wait AWS call counter reached %d, waiting for all background processes to complete...", waitAWSCalls))
		logger.Log("INFO", "All background processes completed, resetting wait counter...")
		waitAWSCalls = 0
		logger.Log("DEBUG", fmt.Sprintf("Final wait AWS call counter reset to: %d", waitAWSCalls))
	} else {
		logger.Log("DEBUG", fmt.Sprintf("Final check: Wait AWS call counter is %d (less than %d), finishing without final wait...", waitAWSCalls, waitThreshold))
	}

	logger.Log("INFO", "Metrics collection completed successfully for all tables")
	fmt.Println("================================================")
	fmt.Println("Completed collection of statistics for all tables")
	fmt.Println("")
	fmt.Println("Collection Summary:")
	fmt.Println("  - 3-Hour Collection: 9 iterations per table (1-second period)")
	fmt.Println("  - 7-Day Collection: 7 iterations per table (60-second period)")
	fmt.Printf("  - Total AWS API calls made: %d\n", totalAWSCalls)
	fmt.Println("================================================")

	return nil
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "get_dynamodb_metrics",
		Short: "Collect DynamoDB metrics from CloudWatch",
		Long: `A comprehensive tool for collecting DynamoDB metrics from CloudWatch.
This tool collects sample counts and P99 latency metrics for all DynamoDB operations
across multiple time periods and regions.`,
		RunE: runMetricsCollection,
	}

	// Add flags
	rootCmd.Flags().StringSliceVarP(&tableNames, "tables", "t", nil, "Comma-separated list of specific tables to process")
	rootCmd.Flags().StringVarP(&awsProfile, "profile", "p", "", "AWS profile to use")
	rootCmd.Flags().StringSliceVarP(&regionsToProbe, "regions", "r", nil, "Comma-separated list of regions to process")
	rootCmd.Flags().BoolVarP(&useInstanceProfile, "instance-profile", "I", false, "Use EC2 Instance Profile for authentication")
	rootCmd.Flags().Int64VarP(&waitThreshold, "wait-threshold", "w", 1000, "Number of AWS calls before waiting")

	// Parse table names from comma-separated string
	rootCmd.PreRun = func(cmd *cobra.Command, args []string) {
		if len(tableNames) == 1 && strings.Contains(tableNames[0], ",") {
			tableNames = strings.Split(tableNames[0], ",")
		}
		if len(regionsToProbe) == 1 && strings.Contains(regionsToProbe[0], ",") {
			regionsToProbe = strings.Split(regionsToProbe[0], ",")
		}
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
