# Dockerfile for building the Moq Relay Server
#
# docker build -t moqrelay -f docker/Dockerfile .
# 
# To run the container:
#
# Using default values (port 4433, logging DBG)
# docker run --rm -p 4433:4433 moqrelay
#
# Override logging level
# docker run --rm -p 4433:4433 -e MOQ_LOG_LEVEL=INFO moqrelay
#
# Override both port and logging
# docker run --rm -p 8080:8080 -e MOQ_PORT=8080 -e MOQ_LOG_LEVEL=INFO moqrelay
#
# Using certs from a specific host directory
# docker run --rm -p 4433:4433 \
#   -v /path/to/your/certs:/certs \
#   moqrelay
#
# Using individual cert files with different names
# docker run --rm -p 4433:4433 \
#   -v /path/to/custom.pem:/certs/certificate.pem \
#   -v /path/to/custom.key:/certs/certificate.key \
#   moqrelay
#
# # Using completely custom paths with environment variables
# docker run --rm -p 4433:4433 \
#   -v /path/to/certs:/custom/certs \
#   -e CERT_FILE=/custom/certs/my-cert.pem \
#   -e KEY_FILE=/custom/certs/my-key.key \
#   moqrelay
#
FROM ubuntu:latest

# Install essential build tools and dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    gcc \
    g++ \
    make \
    doxygen \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /app

# Copy the rest of the project files
COPY . .

# Run the build script (already running as root)
RUN ./build.sh
RUN ./scripts/create-server-certs.sh

# Set default environment variables
ENV MOQ_LOG_LEVEL=DBG
ENV MOQ_PORT=4433

EXPOSE ${MOQ_PORT}

CMD ["sh", "-c", "./_build/bin/moqrelayserver -port ${MOQ_PORT} -cert ./certs/certificate.pem -key ./certs/certificate.key -endpoint \"/moq\" --logging ${MOQ_LOG_LEVEL}"]