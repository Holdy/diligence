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
                    descriptor: `${batcherId} batch of ${currentBatchList.length} items`,
                };
                await this.emitAsync([...currentBatchList], batchMetadata, context);
                currentBatchList.length = 0; // clear batch
            }
        },
        processCompleteAsync: async function (context) {
            const currentBatchList = context[batcherId];
            if (currentBatchList && currentBatchList.length > 0) {
                const batchMetadata = {
                    descriptor: `final ${batcherId} batch of ${currentBatchList.length} items`,
                };
                await this.emitAsync([...currentBatchList], batchMetadata, context);
                currentBatchList.length = 0;
            }
            await this.emitCompleteAsync(context);
        },
    };
}

function fieldBatcher(field_name) {
    const batcherId = `fieldBatcher-${++idSequence}`; // allow multiple batchers per pipeline.

    return {
        processAsync: async function (item, itemMetadata, context) {
            let currentBatchList = context[batcherId];

            if (!currentBatchList) {
                currentBatchList = [];
                context[batcherId] = currentBatchList;
            }
            
            if (currentBatchList.length == 0) {
                currentBatchList.push(item);
            } else {
                const currentBatchDescriptor = currentBatchList[0][field_name];
                if (currentBatchDescriptor === item[field_name]) {
                    // same batch
                    currentBatchList.push(item);
                } else {
                    const batchMetadata = {
                        descriptor: `${batcherId} batch of ${currentBatchList.length} items`,
                    };
                    await this.emitAsync([...currentBatchList], batchMetadata, context);
                    currentBatchList.length = 0; // clear batch

                    currentBatchList.push(item);
                }
            }
        },
        processCompleteAsync: async function (context) {
            const currentBatchList = context[batcherId];
            if (currentBatchList && currentBatchList.length > 0) {
                const batchMetadata = {
                    descriptor: `final ${batcherId} batch of ${currentBatchList.length} items`,
                };
                await this.emitAsync([...currentBatchList], batchMetadata, context);
            }
            await this.emitCompleteAsync(context);
        },
    };
}


function parallelise(numberAtOnce) {

    const parallelList = [];

    return {
        processAsync: async function (item, itemMetadata, context) {

            parallelList.push({item, itemMetadata, context});

            if (parallelList.length === numberAtOnce) {
                const promiseList = parallelList.map(listItem => this.emitAsync(listItem.item, listItem.itemMetadata, listItem.context));
                parallelList.length = 0; // clear batch
                await Promise.all(promiseList);
            }
        },
        processCompleteAsync: async function (context) {
            if (parallelList.length > 0) {
                const promiseList = parallelList.map(listItem => this.emitAsync(listItem.item, listItem.itemMetadata, listItem.context));
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

    // last item is wired to the sink - no need to bind as sink's functions (see end of file) don't use 'this'.
    if (currentItem) {
        currentItem.emitAsync = sink.processAsync;
        currentItem.emitCompleteAsync = sink.processCompleteAsync;
    }

    let pipelineInputSequence = 0;
    const pipelineHandle = {
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
        processEachAsync : async (list, descriptor) => {
            try {
                let itemIndex = 0;
                const context = {};
                const descriptorPrefix = descriptor || 'pipeline-input-list';
    
                for (const listItem of list) {
                    const itemMeta = {
                        descriptor: `${descriptorPrefix}-${++itemIndex}/${list.length}`,
                    };
    
                    logging.logDebug(`processEachAsync() with ${itemMeta.descriptor}`);
                    await firstItem.processAsync(listItem, itemMeta, context);
                }
                await firstItem.processCompleteAsync(context);
            } catch (errorFromPipeline) {
                logging.logError(errorFromPipeline);
                throw errorFromPipeline;
            }   
        }
    };

    return pipelineHandle;
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
module.exports.fieldBatcher = fieldBatcher;
module.exports.parallelise = parallelise;
module.exports.from = from;
