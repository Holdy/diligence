'use strict';

const { Pool } = require('pg');

const grok = require('../grok');
const logging = require('../logging');
const ensure = require('../ensure');
const fcf = require('../firstClassFields');
const pipeline = require('../pipeline');

const PostgresAbstraction = require('./PostgresAbstraction');

async function provideCommandExecutorAsync(credentials, databaseName, usingCallback) {
    return new Promise(async (resolve, reject) => {
        const fields = grok.credentialsFrom(credentials);

        if (!fields) {
            const message = 'No recognised fields in credentials object';
            logging.logError(message);
            return reject(new Error(message));
        }

        let librarySpecificCredentials;
        try {
            librarySpecificCredentials = {
                host: ensure.defined(fields[fcf.host.key]),
                port: ensure.defined(fields[fcf.hostPort.key]),
                user: ensure.defined(fields[fcf.username.key]),
                password: ensure.defined(fields[fcf.password.key]),
                database: ensure.defined(databaseName),
            };
        } catch (err) {
            return reject(err);
        }

        let pool;
        let phase = 'open connection';
        try {
            logging.logInfo(`Opening connection to database '${fields[fcf.host.key]}'`);
            pool = new Pool(librarySpecificCredentials);
            phase = 'provideCommandExecutorAsync(usingCallback()) execution';

            const callbackResult = await usingCallback(new PostgresAbstraction(pool));

            logging.logDebug(`${phase} - complete`);
            closePool(pool, fields);
            resolve(callbackResult);
        } catch (err) {
            const message = `Error during ${phase}`;
            logging.logError(message, err);
            closePool(pool, fields);
            reject(new Error(message, err));
        }
    });
}

function ensurePool(waveMetadata, dbCredentials, databaseName) {
    if (!waveMetadata.pool) {
        waveMetadata.waveNumber++;
        const fields = grok.credentialsFrom(dbCredentials);

        if (!fields) {
            logging.logAndThrowError('No recognised fields in credentials object');
        }

        const librarySpecificCredentials = {
            host: ensure.defined(fields[fcf.host.key]),
            port: ensure.defined(fields[fcf.hostPort.key]),
            user: ensure.defined(fields[fcf.username.key]),
            password: ensure.defined(fields[fcf.password.key]),
            database: ensure.defined(databaseName),
        };

        logging.logInfo(
            `${pipeline.loggingPrefix}sqlUpdateRunner - Opening connection to database '${fields[fcf.host.key]}' for wave #${waveMetadata.waveNumber}`
        );
        waveMetadata.pool = new Pool(librarySpecificCredentials);
        waveMetadata.fields = fields;
    }
}

function sqlUpdateRunner(dbCredentials, databaseName, commandPreparationCallback) {
    let waveMetadata = { waveNumber: 0 };

    return {
        async processAsync(item, itemMetadata, context) {
            logging.logInfo(`${pipeline.loggingPrefix}sqlUpdateRunner - checking just-in-time connection`);
            ensurePool(waveMetadata, dbCredentials, databaseName);

            const parameters = {};
            const command = { parameters: parameters, variables: parameters };
            commandPreparationCallback(item, command);

            const postgresAbstraction = new PostgresAbstraction(waveMetadata.pool);

            try {
                await postgresAbstraction.performUpdateAsync(command.query, parameters);
            } catch (err) {
                logging.logWarning(
                    `Connection to database '${waveMetadata.fields[fcf.host.key]}' will be closed due to error during wave #${waveMetadata.waveNumber}`
                );
                closePool(waveMetadata.pool, waveMetadata.fields);
                throw error;
            }

            await this.emitAsync(item, itemMetadata, context); // item passed on unchanged.
        },
        processCompleteAsync(context) {
            if (waveMetadata.pool) {
                logging.logInfo(
                    `${pipeline.loggingPrefix}sqlUpdateRunner - Connection to database '${
                        waveMetadata.fields[fcf.host.key]
                    }' will be closed due to end of wave #${waveMetadata.waveNumber}`
                );
                closePool(waveMetadata.pool, waveMetadata.fields);
            }
            this.emitCompleteAsync(context);
        },
    };
}

function sqlUpdateExecutor(parameterPreparationCallback) {
    return {
        async processAsync(item, itemMetadata, context) {
            // Hand the pipeline item to callback code, it will return us parameters.
            const parameters = parameterPreparationCallback(item);
            await parameters._executor.performUpdateAsync(parameters._query, parameters);
            await this.emitAsync(item, itemMetadata, context); // Item is passed on untouched.
        },
        processCompleteAsync(context) {
            this.emitCompleteAsync(context);
        },
    };
}

function closePool(pool, fields) {
    if (pool) {
        logging.logInfo(`Closing connection to database '${fields[fcf.host.key]}'`);
        pool.end();
    }
}

module.exports.provideCommandExecutorAsync = provideCommandExecutorAsync;
module.exports.sqlUpdateExecutor = sqlUpdateExecutor;
module.exports.sqlUpdateRunner = sqlUpdateRunner;
