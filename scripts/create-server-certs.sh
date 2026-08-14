#!/usr/bin/env bash

# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the Apache 2.0 license found in the
# LICENSE file in the root directory of this source tree.

set -euo pipefail

# Resolve relative to this script, not the caller's working directory, so the
# certs always land in moxygen/certs/ regardless of where it is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="${SCRIPT_DIR}/../certs"

mkdir -p "${CERTS_DIR}"

# Generate a self-signed certificate and its private key
openssl req -newkey rsa:2048 -nodes \
  -keyout "${CERTS_DIR}/certificate.key" \
  -x509 -out "${CERTS_DIR}/certificate.pem" \
  -subj '/CN=Test Certificate' \
  -addext "subjectAltName = DNS:localhost"

echo "Wrote ${CERTS_DIR}/certificate.pem and ${CERTS_DIR}/certificate.key"
