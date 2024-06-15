#!/usr/bin/env bash

# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# Create certs dir if it does not exists
mkdir -p ../certs

# Generate publi - private key
openssl req -newkey rsa:2048 -nodes -keyout ../certs/certificate.key -x509 -out ../certs/certificate.pem -subj '/CN=Test Certificate' -addext "subjectAltName = DNS:localhost"
