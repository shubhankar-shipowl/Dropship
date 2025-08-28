#!/usr/bin/env node

/**
 * Plesk-compatible entry point for Node.js applications
 * This file serves as the main entry point that Plesk expects
 */

import('./dist/index.js')
  .then(() => {
    console.log('Application started successfully');
  })
  .catch((error) => {
    console.error('Failed to start application:', error);
    process.exit(1);
  });