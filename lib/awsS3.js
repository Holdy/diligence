'use strict';

const aws = require('aws-sdk');
const { defaultAws_httpOptions } = require('./config');
const readline = require('readline');
const zlib = require('zlib');

const s3 = new aws.S3({ httpOptions: defaultAws_httpOptions });

const ensure = require('./ensure');
const logging = require('./logging');
const pipeline = require('./pipeline');

function resolveObjectReference(item) {
    const result = {};

    if (item.Records && item.Records[0].s3) {
        const s3Details = item.Records[0].s3;
        if (s3Details.bucket && s3Details.bucket.name) {
            result.bucket = s3Details.bucket.name;
        }
        if (s3Details.object && s3Details.object.key) {
            result.key = s3Details.object.key;
        }
    }

    ensure.defined(result.bucket, 'Could not determine the S3-Object.bucket');
    ensure.defined(result.key, 'Could not determine the S3-Object.key');

    if (result.key.endsWith('.gz')) {
        result.isGzipped = true;
    }

    return result;
}

function callAsPromiseAsync(apiCall, params) {
    return new Promise((resolve, reject) => {
        apiCall(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}

async function handlePagedCallsAsync(callParameters, itemCallbackAsync) {
    let pagingComplete = false;

    return new Promise(async (resolve, reject) => {
        while (!pagingComplete) {
            try {
                const result = await callAsPromiseAsync(s3.listObjectsV2.bind(s3), callParameters);

                for (const s3Object of result.Contents) {
                    await itemCallbackAsync(s3Object);
                }

                if (result.IsTruncated) {
                    callParameters.ContinuationToken = result.NextContinuationToken;
                } else {
                    pagingComplete = true;
                    resolve();
                }
            } catch (err) {
                pagingComplete = true;
                reject(err);
            }
        }
    });
}

function objectLister() {
    return {
        itemCount: 0,
        processAsync: async function (bucketReference, itemMeta, context) {
            const params = {
                Bucket: bucketReference.bucket,
            };
            await handlePagedCallsAsync(params, async (item) => {
                this.itemCount++;
                const newItemMeta = {
                    descriptor: `Item ${this.itemCount} from bucket: ${bucketReference.bucket}`,
                };
                item.Bucket = params.Bucket;
                await this.emitAsync(item, newItemMeta, context);
            });
        },
        processCompleteAsync: async function (context) {
            logging.logInfo(`${pipeline.loggingPrefix}objectLister - Processed ${this.itemCount} bucket objects`);
            await this.emitCompleteAsync(context);
        },
    };
}

function fileLineEmitter() {
    return {
        processAsync: async function (fileReference, itemMeta, context) {
            let S3Reference = resolveObjectReference(fileReference);

            return new Promise(async (resolve, reject) => {
                const readStream = s3.getObject({ Bucket: S3Reference.bucket, Key: S3Reference.key }).createReadStream();

                const dataStream = S3Reference.isGzipped ? readStream.pipe(zlib.createGunzip()) : readStream;
                const lineReader = readline.createInterface({ input: dataStream });

                let lineIndex = 0;

                for await (const line of lineReader) {
                    lineIndex++;
                    await this.emitAsync(line, { descriptor: `file line ${lineIndex}` }, context);
                }

                logging.logInfo(
                    `${pipeline.loggingPrefix}fileLineEmitter - Finished read of ${lineIndex} lines from S3-Object - ${S3Reference.bucket} ${S3Reference.key}`
                );
                resolve();
    
            });
        },
        processCompleteAsync: async function (context) {
            await this.emitCompleteAsync(context);
        },
    };
}

module.exports.fileLineEmitter = fileLineEmitter;
module.exports.objectLister = objectLister;
