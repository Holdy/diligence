'use strict';

const AWS = require('aws-sdk');

const ensure = require('./ensure');


const SQS = new AWS.SQS({apiVersion: '2012-11-05'});

function sendAsync(options) {
    return new Promise((resolve, reject) => {
        ensure.defined(options.MessageBody, 'MessageBody is required by SQS');
        ensure.defined(options.QueueUrl,    'QueueUrl is required by SQS');
    
        SQS.sendMessage(options, (err, result) => {
            if (err) {
                reject(err);
            } else {
                resolve(result);
            }
        });
    
    });
}


module.exports.sendAsync = sendAsync;