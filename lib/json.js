'use strict';

const pipeline = require('./pipeline');
const logging = require('./logging');

function parser() {
    return {
        metrics: { success: 0, error: 0 },
        processAsync: async function (item, itemMetadata, context) {
            const descriptor = logging.descriptorOrDefault(item, itemMetadata);

            try {
                const parsedItem = JSON.parse(item);
                this.metrics.success++;
                itemMetadata.descriptor = `json from ${descriptor}`;

                await this.emitAsync(parsedItem, itemMetadata, context);
            } catch (err) {
                this.metrics.error++;
                const message = `Failed to parse ${descriptor}`;
                logging.logAndThrowError(message);
            }
        },
        processCompleteAsync: async function (context) {
            logging.logInfo(`${pipeline.loggingPrefix}json.parser - successfully parsed ${this.metrics.success} items, Errored on ${this.metrics.error} items`);
            await this.emitCompleteAsync(context);
        },
    };
}

function attemptCoalesce(target, pathSteps) {
    let current = target;
    try {
        pathSteps.forEach((pathStep) => (current = current[pathStep]));
    } catch (err) {
        current = null;
    }
    return current || current === 0 || current === false || current === '' ? current : null;
}

function coalesce(target, ...paths) {
    let result = null;
    for (let index = 0; index < paths.length; index++) {
        // to allow break
        result = attemptCoalesce(target, paths[index]);
        if (result || result === false || result === 0) {
            break;
        }
    }

    return result;
}

function prettyPrinter() {
    return {
        processAsync: async function (item, itemMetadata, context) {
            const descriptor = (itemMetadata && itemMetadata.descriptor) || 'unnamed-item';
            try {
                const text = JSON.stringify(item, null, 3);
                if (!text) {
                    throw new Error('Item was not stringified.');
                }
                await this.emitAsync(text, { descriptor: `stringified-${descriptor}` }, context);
            } catch (err) {
                // There's not much that stringify will fail on!
                logging.logAndThrowError(`Failed to stringify [${descriptor}]`);
            }
        },
        processCompleteAsync: async function (context) {
            await this.emitCompleteAsync(context);
        },
    };
}

module.exports.parser = parser;
module.exports.prettyPrinter = prettyPrinter;
module.exports.coalesce = coalesce;
