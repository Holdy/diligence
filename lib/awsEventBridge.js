'use strict';

const aws = require('aws-sdk');
const { defaultAws_httpOptions } = require('./config');
const logging = require('./logging');

const eventBridge = new aws.EventBridge(
    {region: 'eu-west-1',
    httpOptions: defaultAws_httpOptions});

function putEventsAsync(params) {
    return new Promise((resolve, reject) => {
        eventBridge.putEvents(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}

module.exports.putEventsAsync = putEventsAsync;

function eventPutter() {
    return {
        processAsync: async function (itemOrList, itemMeta, context) {
            const itemList = Array.isArray(itemOrList) ? itemOrList : [itemOrList];
            const params = {
                Entries: itemList,
            };

            const putEventsResult = await module.exports.putEventsAsync(params);
            logging.logTrace('putEvents() result', putEventsResult);
            
            // input is sent onward unchanged.
            await this.emitAsync(itemOrList, itemMeta, context);
        },
        processCompleteAsync: async function (context) {
            await this.emitCompleteAsync(context);
        },
    };
}

module.exports.eventPutter = eventPutter
