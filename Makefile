# Makefile for DynamoDB Metrics Collection Tool

# Binary name
BINARY_NAME=get_dynamodb_metrics

# Go version
GO_VERSION=1.21

# Build flags
LDFLAGS=-ldflags "-X main.Version=$(shell git describe --tags --always --dirty 2>/dev/null || echo 'dev')"

# Default target
.DEFAULT_GOAL := build

# Build the binary
.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	@go build $(LDFLAGS) -o $(BINARY_NAME) .

# Build for multiple platforms
.PHONY: build-all
build-all: build-linux build-darwin build-windows

build-linux:
	@echo "Building for Linux..."
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 .

build-darwin:
	@echo "Building for macOS..."
	@GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 .

build-windows:
	@echo "Building for Windows..."
	@GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe .

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	@go mod tidy
	@go mod download

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	@go test -v ./...

# Run with race detection
.PHONY: test-race
test-race:
	@echo "Running tests with race detection..."
	@go test -race -v ./...

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(BINARY_NAME)
	@rm -f $(BINARY_NAME)-linux-amd64
	@rm -f $(BINARY_NAME)-darwin-amd64
	@rm -f $(BINARY_NAME)-windows-amd64.exe
	@rm -rf dynamo_metrics_logs_*

# Run the tool (example with common options)
.PHONY: run
run: build
	@echo "Running $(BINARY_NAME)..."
	@./$(BINARY_NAME)

# Run with specific tables
.PHONY: run-tables
run-tables: build
	@echo "Running with specific tables..."
	@./$(BINARY_NAME) -t table1,table2

# Run with specific regions
.PHONY: run-regions
run-regions: build
	@echo "Running with specific regions..."
	@./$(BINARY_NAME) -r us-east-1,us-west-2

# Run with AWS profile
.PHONY: run-profile
run-profile: build
	@echo "Running with AWS profile..."
	@./$(BINARY_NAME) -p default

# Run with instance profile
.PHONY: run-instance
run-instance: build
	@echo "Running with instance profile..."
	@./$(BINARY_NAME) -I

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Run linter
.PHONY: lint
lint:
	@echo "Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

# Check for security vulnerabilities
.PHONY: security
security:
	@echo "Checking for security vulnerabilities..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not found. Install with: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest"; \
	fi

# Generate documentation
.PHONY: docs
docs:
	@echo "Generating documentation..."
	@go doc -all ./...

# Install the binary to system path
.PHONY: install
install: build
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	@sudo cp $(BINARY_NAME) /usr/local/bin/
	@sudo chmod +x /usr/local/bin/$(BINARY_NAME)

# Uninstall the binary
.PHONY: uninstall
uninstall:
	@echo "Uninstalling $(BINARY_NAME)..."
	@sudo rm -f /usr/local/bin/$(BINARY_NAME)

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build        - Build the binary"
	@echo "  build-all    - Build for Linux, macOS, and Windows"
	@echo "  deps         - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  test-race    - Run tests with race detection"
	@echo "  clean        - Clean build artifacts"
	@echo "  run          - Run the tool"
	@echo "  run-tables   - Run with specific tables"
	@echo "  run-regions  - Run with specific regions"
	@echo "  run-profile  - Run with AWS profile"
	@echo "  run-instance - Run with instance profile"
	@echo "  fmt          - Format code"
	@echo "  lint         - Run linter"
	@echo "  security     - Check for security vulnerabilities"
	@echo "  docs         - Generate documentation"
	@echo "  install      - Install binary to system path"
	@echo "  uninstall    - Uninstall binary"
	@echo "  help         - Show this help message"

# Development setup
.PHONY: dev-setup
dev-setup: deps
	@echo "Setting up development environment..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	@if ! command -v gosec >/dev/null 2>&1; then \
		echo "Installing gosec..."; \
		go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest; \
	fi
	@echo "Development environment setup complete!"

# Check Go version
.PHONY: check-go
check-go:
	@echo "Checking Go version..."
	@go version
	@if [ "$$(go version | awk '{print $$3}' | sed 's/go//')" != "$(GO_VERSION)" ]; then \
		echo "Warning: Expected Go version $(GO_VERSION), but found $$(go version | awk '{print $$3}' | sed 's/go//')"; \
	fi

# Pre-commit checks
.PHONY: pre-commit
pre-commit: check-go fmt lint test
	@echo "Pre-commit checks completed successfully!"

# Release build
.PHONY: release
release: clean build-all
	@echo "Creating release archive..."
	@tar -czf $(BINARY_NAME)-release.tar.gz \
		$(BINARY_NAME)-linux-amd64 \
		$(BINARY_NAME)-darwin-amd64 \
		$(BINARY_NAME)-windows-amd64.exe \
		README.md \
		go.mod \
		go.sum
	@echo "Release archive created: $(BINARY_NAME)-release.tar.gz" 