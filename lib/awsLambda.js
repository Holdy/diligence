'use strict';

const aws = require('aws-sdk');
const lambda = new aws.Lambda();

const logging = require('./logging');
const pipeline = require('./pipeline');
const json = require('./json');

function invokeLambdaAsync(params) {
    return new Promise((resolve, reject) => {
        lambda.invoke(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}

function synchronousInvoker(lambdaAndPayloadCallback) {
    const stats = { invocationCount: 0, success: 0, failure: 0 };
    return {
        async processAsync(item, itemMetadata, context) {
            const callData = {};
            lambdaAndPayloadCallback(item, callData);

            const internalPayload = json.coalesce(callData, ['Payload'], ['input']);
            const payload = JSON.stringify(internalPayload);

            const params = {
                FunctionName: json.coalesce(callData, ['FunctionName'], ['lambda']),
                InvocationType: 'RequestResponse',
                LogType: 'Tail',
                Payload: payload,
            };

            stats.invocationCount++;
            const result = await invokeLambdaAsync(params);

            if (result.StatusCode === 200) {
                stats.success++;
            } else {
                stats.failure++;
                const errorMessage = `Non 200 StatusCode ${result.StatusCode}`;
                logging.logWarn(errorMessage);
                throw new Error(errorMessage);
            }

            await this.emitAsync(item, itemMetadata, context); // item passed on unchanged.
        },
        processCompleteAsync(context) {
            logging.logInfo(
                `${pipeline.loggingPrefix}synchronousInvoker - Success: ${stats.success} Failure: ${stats.failure} out of ${stats.invocationCount} `
            );
            this.emitCompleteAsync(context);
        },
    };
}

module.exports.synchronousInvoker = synchronousInvoker;
