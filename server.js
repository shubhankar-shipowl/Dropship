#!/usr/bin/env node

/**
 * Alternative Plesk entry point (CommonJS compatible)
 * Some Plesk configurations prefer this naming convention
 */

require = require('esm')(module);
module.exports = require('./dist/index.js');