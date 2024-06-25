#!/usr/bin/env node
const { postgraphile } = require("postgraphile");

const connectionString = process.argv[2];
const schemas = process.argv[3].split(",");
const options = {
    watch: process.argv.includes("--watch"),
    disableDefaultMutations: true,
    dynamicJson: true,
    cors: true,
    retryOnInitFail: true,
    disableGraphiql: true,
    enableQueryBatching: true,
    noIgnoreIndexes: true,
    enhanceGraphiql: false,
    port: process.argv.includes("--port") ? parseInt(process.argv[process.argv.indexOf("--port") + 1], 10) : 5005,
};

postgraphile(connectionString, schemas, options);
