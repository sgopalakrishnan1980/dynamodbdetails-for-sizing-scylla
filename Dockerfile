# Multi-stage build for Go DynamoDB Metrics Collection Tool

# Build stage
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Install git and ca-certificates (needed for go mod download)
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o get_dynamodb_metrics .

# Runtime stage
FROM alpine:latest

# Install ca-certificates, curl, and AWS CLI
RUN apk --no-cache add ca-certificates curl bash aws-cli

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/get_dynamodb_metrics .

# Create directory for logs and AWS config
RUN mkdir -p /app/logs /home/appuser/.aws && \
    chown -R appuser:appgroup /app /home/appuser/.aws

# Copy AWS configuration script
COPY --chown=appuser:appgroup docker-entrypoint.sh /app/

# Make entrypoint script executable
RUN chmod +x /app/docker-entrypoint.sh

# Switch to non-root user
USER appuser

# Set environment variables
ENV AWS_SDK_LOAD_CONFIG=1
ENV AWS_CONFIG_FILE=/home/appuser/.aws/config
ENV AWS_SHARED_CREDENTIALS_FILE=/home/appuser/.aws/credentials

# Expose port (if needed for health checks)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Use custom entrypoint script
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command
CMD ["--help"]

