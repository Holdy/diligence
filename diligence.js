'use strict';

const fs = require('fs');

const logging = require('./lib/logging');
const environment = require('./lib/environment');
const ensure = require('./lib/ensure');
const contentMap = require('./lib/contentMap');
const awsSecretsManager = require('./lib/awsSecretsManager');
const pipeline = require('./lib/pipeline');
const json = require('./lib/json');
const awsS3 = require('./lib/awsS3');
const awsEventBridge = require('./lib/awsEventBridge');
const awsSqs = require('./lib/awsSqs');
const awsLambda = require('./lib/awsLambda');
const postgres = require('./lib/postgres/postgres');

function readTextFile(relativePath) {
    ensure.defined(relativePath, 'readTextFile - no file path was given');
    try {
        logging.logInfo(`Reading file: ${relativePath}`);
        let content = fs.readFileSync(relativePath).toString();

        let metadata = { fileName: relativePath };
        contentMap.set(content, metadata);

        return content;
    } catch (err) {
        logging.logAndThrowError(`Error while reading file: ${relativePath}`, err);
    }
}

module.exports.logError = logging.logError;
module.exports.environment = environment;
module.exports.logging = logging;
module.exports.readTextFile = readTextFile;
module.exports.awsSecretsManager = awsSecretsManager;
module.exports.pipeline = pipeline;
module.exports.json = json;
module.exports.awsS3 = awsS3;
module.exports.awsEventBridge = awsEventBridge;
module.exports.awsSqs = awsSqs;
module.exports.awsLambda = awsLambda;
module.exports.postgres = postgres;
module.exports.ensure = ensure;
