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

  # Stage 1: Build
  FROM ubuntu:latest AS builder

  # Install python3 and sudo (getdeps.py install-system-deps requires sudo)
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

  RUN ./build/fbcode_builder/getdeps.py build --allow-system-packages --verbose moxygen 2>&1 || \
      (echo "=== Build failed, showing logs ===" && \
       find /tmp -name "*.log" -type f -exec echo "=== {} ===" \; -exec cat {} \; 2>/dev/null; \
       exit 1)

  # Stage the binary and getdeps-built shared libs for the runtime image
  RUN INST_DIR=$(./build/fbcode_builder/getdeps.py show-inst-dir moxygen) && \
      mkdir -p /staging/bin /staging/lib && \
      cp $INST_DIR/bin/moqrelayserver /staging/bin/ && \
      cp /tmp/fbcode_builder_getdeps-ZappZbuildZfbcode_builder-root/installed/*/lib/libglog.so* /staging/lib/ && \
      cp /tmp/fbcode_builder_getdeps-ZappZbuildZfbcode_builder-root/installed/*/lib/libunwind.so* /staging/lib/

  # Stage 2: Runtime
  FROM ubuntu:latest

  RUN apt-get update && apt-get install -y --no-install-recommends \
      libsodium23 \
      libdouble-conversion3 \
      libevent-2.1-7 \
      libgflags2.2 \
      libssl3 \
      libsnappy1v5 \
      zlib1g \
      ca-certificates \
      openssl \
      && rm -rf /var/lib/apt/lists/*

  COPY --from=builder /staging/bin/moqrelayserver /usr/local/bin/
  COPY --from=builder /staging/lib/ /usr/local/lib/

  # Generate default self-signed certs
  RUN mkdir -p /certs && \
      openssl req -newkey rsa:2048 -nodes -keyout /certs/certificate.key \
      -x509 -out /certs/certificate.pem -subj '/CN=Test Certificate' \
      -addext "subjectAltName = DNS:localhost"

  ENV LD_LIBRARY_PATH=/usr/local/lib
  ENV MOQ_LOG_LEVEL=DBG
  ENV MOQ_PORT=4433
  ENV CERT_FILE=/certs/certificate.pem
  ENV KEY_FILE=/certs/certificate.key
  ENV MOQ_ENDPOINT=/moq

  EXPOSE ${MOQ_PORT}/udp

  CMD ["sh", "-c", "moqrelayserver -port ${MOQ_PORT} -cert ${CERT_FILE} -key ${KEY_FILE} -endpoint \"${MOQ_ENDPOINT}\" --logging ${MOQ_LOG_LEVEL}"]
