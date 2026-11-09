#!/usr/bin/env node
/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const fs = require('fs');
const path = require('path');
const MoQParser = require('./html/moq-parser');
const parser = new MoQParser();
const sampleData = parser.generateExampleData().filter(event => event.vantagePoint === 'client');
const jsonOutput = sampleData.map(event => JSON.stringify(event, null, 2)).join('\n') + '\n';
const outputPath = path.join(__dirname, 'examples/sample_moq.mlog');
fs.writeFileSync(outputPath, jsonOutput);

console.log(`Generated serialized sample data at: ${outputPath}`);
console.log(`Events generated: ${sampleData.length}`);
