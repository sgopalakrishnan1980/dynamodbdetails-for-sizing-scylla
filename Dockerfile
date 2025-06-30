FROM debian:bookworm-slim

# Install required dependencies
RUN apt-get update && apt-get install -y \
    curl unzip jq git groff less python3-pip ca-certificates vim-tiny && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Clone the GitHub repository
RUN git clone https://github.com/sgopalakrishnan1980/dynamodbdetails-for-sizing-scylla.git /app/dynamodbdetails

WORKDIR /app/dynamodbdetails

CMD [ "bash" ]

