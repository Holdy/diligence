'use strict';

const logging = require('./logging');

function definedImplementation(value) {
    return value || value === false || value === 0;
}

function defined(value, message) {
    if (!definedImplementation(value)) {
        logging.logAndThrowError(message);
    }
    return value;
}

function inList(item, array, message) {
    if (!array.includes(item)) {
        logging.logAndThrowError(message);
    }
}

function integer(value, message) {
    try {
        return parseInt(value);
    } catch (err) {
        logging.logAndThrowError(message, err);
    }
}

module.exports.defined = defined;
module.exports.integer = integer;
module.exports.inList = inList;
