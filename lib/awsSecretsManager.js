'use strict';

const AWS = require('aws-sdk');
const { defaultAws_httpOptions } = require('./config');

function getItemAsync(itemId) {
    const secretsManager = new AWS.SecretsManager({
        region: 'eu-west-1',
        httpOptions: defaultAws_httpOptions,
    });
    return new Promise((resolve, reject) => {
        secretsManager.getSecretValue({ SecretId: itemId }, (err, result) => {
            if (err) {
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

module.exports.getItemAsync = getItemAsync;
