#!/usr/bin/env node
/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Temporary script to generate serialized sample data

// Mock jQuery for server-side execution
global.$ = {
    ready: function(callback) { callback(); }
};

// Load the MoQ parser
const fs = require('fs');
const path = require('path');

// Read and evaluate the parser file
const parserCode = fs.readFileSync(path.join(__dirname, 'html/moq-parser.js'), 'utf8');
eval(parserCode);

// Generate sample data
const parser = new MoQParser();
const sampleData = parser.generateExampleData();

// Pretty print the JSON
const jsonOutput = JSON.stringify(sampleData, null, 2);

// Write to file
const outputPath = path.join(__dirname, 'examples/sample_serialized.qlog');
fs.writeFileSync(outputPath, jsonOutput);

console.log(`Generated serialized sample data at: ${outputPath}`);
console.log(`Events generated: ${sampleData.traces[0].events.length}`);
