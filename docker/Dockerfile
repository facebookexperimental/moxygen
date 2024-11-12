# docker/Dockerfile
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