# Dockerfile for building the Moq Relay Server
#
# docker build -t moqrelay -f docker/Dockerfile .
#
# To run the container:
#
# Using default values (udp port 4433, logging DBG)
# docker run --rm -p 4433:4433/udp moqrelay
#
# Override logging level
# docker run --rm -p 4433:4433/udp -e MOQ_LOG_LEVEL=INFO moqrelay
#
# Override both port and logging
# docker run --rm -p 8080:8080/udp -e MOQ_PORT=8080 -e MOQ_LOG_LEVEL=INFO moqrelay
#
# Using certs from a specific host directory
# docker run --rm -p 4433:4433/udp \
#   -v /path/to/your/certs:/certs \
#   moqrelay
#
# Using individual cert files with different names
# docker run --rm -p 4433:4433/udp \
#   -v /path/to/custom.pem:/certs/certificate.pem \
#   -v /path/to/custom.key:/certs/certificate.key \
#   moqrelay
#
# # Using completely custom paths with environment variables
# docker run --rm -p 4433:4433/udp \
#   -v /path/to/certs:/custom/certs \
#   -e CERT_FILE=/custom/certs/my-cert.pem \
#   -e KEY_FILE=/custom/certs/my-key.key \
#   moqrelay
#
FROM ubuntu:latest

# Install python3 (getdeps.py handles all other build dependencies)
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    gcc \
    g++ \
    make \
    libssl-dev \
    m4 \
    doxygen \
    python3 \
    python3-pip \
    sudo

# Allow pip to install packages system-wide (safe in Docker container)                        
ENV PIP_BREAK_SYSTEM_PACKAGES=1 

# Set up working directory
WORKDIR /app

# Copy the rest of the project files
COPY . .

# Run the build script using getdeps.py (already running as root)
# First install system dependencies to reduce compilation
RUN ./build/fbcode_builder/getdeps.py install-system-deps --recursive moxygen                                                                           

# Limit parallelism to 2 jobs to avoid OOM on GitHub Actions runners (7GB RAM)
RUN ./build/fbcode_builder/getdeps.py build --allow-system-packages --num-jobs 2 --verbose moxygen 2>&1 || \
    (echo "=== Build failed, showing logs ===" && \
     find /tmp -name "*.log" -type f -exec echo "=== {} ===" \; -exec cat {} \; 2>/dev/null; \
     exit 1)
RUN ./scripts/create-server-certs.sh

# Set default environment variables
ENV MOQ_LOG_LEVEL=DBG
ENV MOQ_PORT=4433
ENV CERT_FILE=./certs/certificate.pem
ENV KEY_FILE=./certs/certificate.key
ENV MOQ_ENDPOINT=/moq

EXPOSE ${MOQ_PORT}/udp

CMD ["sh", "-c", "eval $(./build/fbcode_builder/getdeps.py env moxygen) && INST_DIR=$(./build/fbcode_builder/getdeps.py show-inst-dir moxygen) && $INST_DIR/bin/moqrelayserver -port ${MOQ_PORT} -cert ${CERT_FILE} -key ${KEY_FILE} -endpoint \"${MOQ_ENDPOINT}\" --logging ${MOQ_LOG_LEVEL}"]
