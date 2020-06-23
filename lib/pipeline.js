'use strict';

const logging = require('./logging');

const loggingPrefix = 'p: ';
let idSequence = 0;

function batcher(batchSize) {
    const batcherId = `batcher-${++idSequence}`; // allow multiple batchers per pipeline.

    return {
        processAsync: async function (item, itemMetadata, context) {
            let currentBatchList = context[batcherId];

            if (!currentBatchList) {
                currentBatchList = [];
                context[batcherId] = currentBatchList;
            }

            currentBatchList.push(item);

            if (currentBatchList.length === batchSize) {
                const batchMetadata = {
                    descriptor: `batch of ${currentBatchList.length} items`,
                };
                await this.emitAsync(currentBatchList, batchMetadata, context);
                currentBatchList.length = 0; // clear batch
            }
        },
        processCompleteAsync: async function (context) {
            const currentBatchList = context[batcherId];
            if (currentBatchList && currentBatchList.length > 0) {
                const batchMetadata = {
                    descriptor: `final batch of ${currentBatchList.length} items`,
                };
                await this.emitAsync(currentBatchList, batchMetadata, context);
            }
            await this.emitCompleteAsync();
        },
    };
}

function parallelise(numberAtOnce) {

    const parallelList = [];

    return {
        processAsync: async function (item, itemMetadata, context) {

            parallelList.push({item, itemMetadata, context});

            if (parallelList.length === numberAtOnce) {
                const promiseList = parallelList.map(listItem => this.emitAsync(listItem.item, listItem.itemMeta, listItem.context));
                parallelList.length = 0; // clear batch
                await Promise.all(promiseList);
            }
        },
        processCompleteAsync: async function (context) {
            if (parallelList.length > 0) {
                const promiseList = parallelList.map(listItem => this.emitAsync(listItem.item, listItem.itemMeta, listItem.context));
                parallelList.length = 0; // clear batch
                await Promise.all(promiseList);
            }
            await this.emitCompleteAsync();
        },
    };
}

function arraySplitter() {
    return {
        processAsync: async function (array, itemMetadata, context) {
            let index = 0;
            for (const item of array) {
                await this.emitAsync(item, { descriptor: `${itemMetadata.descriptor}[${index++}]` }, context);
            }
        },
        processCompleteAsync: async function (context) {
            await this.emitCompleteAsync(context);
        },
    };
}

function from() {
    let firstItem, previousItem, currentItem;
    for (let index = 0; index < arguments.length; index++) {
        currentItem = arguments[index];
        if (firstItem) {
            previousItem.emitAsync = currentItem.processAsync.bind(currentItem);
            previousItem.emitCompleteAsync = currentItem.processCompleteAsync.bind(currentItem);
        } else {
            firstItem = currentItem;
        }
        previousItem = currentItem;
    }

    // last item is wired to the sink - no need to bind as sink's functions don't use 'this'.
    currentItem.emitAsync = sink.processAsync;
    currentItem.emitCompleteAsync = sink.processCompleteAsync;

    let pipelineInputSequence = 0;
    return {
        processAsync: async (item, descriptor) => {
            try {
                const itemMeta = {
                    descriptor: descriptor || `pipeline-input-${++pipelineInputSequence}`,
                };
                logging.logInfo(`${loggingPrefix}Started processing ${itemMeta.descriptor}`);

                const context = {};
                await firstItem.processAsync(item, itemMeta, context);
                await firstItem.processCompleteAsync(context);

                logging.logInfo(`${loggingPrefix}Finished processing ${itemMeta.descriptor}`);
            } catch (errorFromPipeline) {
                logging.logError(errorFromPipeline);
                throw errorFromPipeline;
            }
        },
    };
}

function filterMap(callback) {
    const descriptor = callback.name || 'unnamed';
    const stats = { total: 0, kept: 0, dropped: 0 };
    return {
        processAsync: async function (item, itemMetadata, context) {
            stats.total++;
            const result = callback(item);

            if (result) {
                stats.kept++;
                await this.emitAsync(result, itemMetadata, context);
            } else {
                stats.dropped++;
            }
        },
        processCompleteAsync: async function (context) {
            logging.logInfo(`${loggingPrefix}filterMap - [${descriptor}] stats: ${JSON.stringify(stats)}`);
            await this.emitCompleteAsync(context);
        },
    };
}

const sink = {
    processAsync: (item, itemMetadata, context) => {
        // No-Op.
    },
    processCompleteAsync(context) {
        // No-Op.
    },
};

module.exports.loggingPrefix = loggingPrefix;
module.exports.arraySplitter = arraySplitter;
module.exports.filterMap = filterMap;
module.exports.batcher = batcher;
module.exports.parallelise = parallelise;
module.exports.from = from;
