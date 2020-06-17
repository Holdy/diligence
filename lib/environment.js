'use strict';

const logging = require('./logging');
const ensure = require('./ensure');

function getInteger(parameters) {
    ensure.defined(parameters.name, 'getIntegerFromEnvironment() requires parameters.name');
    const textValue = exports.getText(parameters);

    return ensure.integer(textValue);
}

function getText(parameters) {
    ensure.defined(parameters.name, 'getText() requires parameters.name');
    const valueFromEnvironment = process.env[parameters.name];

    if (valueFromEnvironment && parameters.allow) {
        ensure.inList(
            valueFromEnvironment,
            parameters.allow,
            `Environment variable ${parameters.name} should be one of [${parameters.allow}] but was [${valueFromEnvironment}]`
        );
    }

    const metaResult = {};

    if (valueFromEnvironment) {
        metaResult.value = valueFromEnvironment;
        metaResult.message = `Read environment variable ${parameters.name} = [${valueFromEnvironment}]`;
    } else {
        metaResult.value = ensure.defined(parameters.default, `Required environment variable ${parameters.name} was not found and has no default.`);
        metaResult.message = `Environment variable ${parameters.name} not defined, using default [${parameters.default}]`;
    }

    logging.logInfo(metaResult.message);
    return metaResult.value;
}

module.exports.getText = getText;
module.exports.getInteger = getInteger;
