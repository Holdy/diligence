'use strict';

const ensure = require('./ensure');

const levels = ['debug', 'info', 'warn', 'error'];

let logTarget = console;
const status = {
    debugActive: true,
    infoActive: true,
    warnActive: true,
    errorActive: true,
};

function setLevel(level) {
    const cleanLevel = (level || '').toLowerCase();
    ensure.inList(cleanLevel, levels, `Logging level should be one of [${levels}] but was [${cleanLevel}]`);
    let aboveLevel = false;
    levels.forEach((testLevel) => {
        if (testLevel == cleanLevel) {
            aboveLevel = true;
        }
        status[`${testLevel}Active`] = aboveLevel;
    });
    status.level = cleanLevel;
}

function logError(message, params) {
    if (!status.errorActive) return;

    if (params && params.length > 0) {
        logTarget.error(message, params);
    } else {
        logTarget.error(message);
    }
}

function logInfo(message, params) {
    if (!status.infoActive) return;

    if (params && params.length > 0) {
        logTarget.info(message, params);
    } else {
        logTarget.info(message);
    }
}
function logWarn(message, params) {
    if (!status.warnActive) return;

    if (params && params.length > 0) {
        logTarget.warn(message, params);
    } else {
        logTarget.warn(message);
    }
}

function ifLogDebug(callback) {
    if (!status.debugActive) return;
    module.exports.logDebug(callback());
}

function logDebug(message, params) {
    if (!status.debugActive) return;

    if (params && params.length > 0) {
        logTarget.debug(message, params);
    } else {
        logTarget.debug(message);
    }
}

function logAndThrowError(message, params) {
    module.exports.logError(message, params);
    throw new Error(message, params);
}
/**************************
 * Pipeline components
 *************************/
function debugLogger() {
    return {
        processAsync: async function (item, itemMetadata, context) {
            logDebug(itemMetadata.descriptor || 'undefined-item');
            logDebug(item);
            await this.emitAsync(item, itemMetadata, context);
        },
        processCompleteAsync: async function (context) {
            await this.emitCompleteAsync(context);
        },
    };
}

function descriptorOrDefault(item, itemMetadata) {
    return (itemMetadata && itemMetadata.descriptor) || 'unnamed-item';
}

module.exports.logError = logError;
module.exports.logInfo = logInfo;
module.exports.logWarn = logWarn;
module.exports.logDebug = logDebug;
module.exports.ifLogDebug = ifLogDebug;
module.exports.logAndThrowError = logAndThrowError;
module.exports.setLevel = setLevel;
module.exports.status = status;
module.exports.debugLogger = debugLogger;
module.exports.descriptorOrDefault = descriptorOrDefault;
module.exports.specification = {
    name: 'LOGGING_LEVEL',
    default: 'info',
    allow: levels,
};
