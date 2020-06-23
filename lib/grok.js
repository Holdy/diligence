'use strict';

const logging = require('./logging');
const fc = require('./firstClassFields');

function attemptParseAsJson(text, descriptor) {
    try {
        return JSON.parse(text);
    } catch (errorWithPossiblySensitiveInformation) {
        logging.logDebug(`Attempted to parse ${descriptor || 'text-value'} as JSON but failed.`);
    }
}

function credentialsFrom(data) {
    let result;
    if (data) {
        if (data.SecretString) {
            const parsedJson = attemptParseAsJson(data.SecretString, 'credentials-data');
            result = parsedJson ? credentialsFromObject(parsedJson) : logging.logAndThrowError('Failed to determine credentials');
        } else {
            return credentialsFromObject(data);
        }
    }
    return result;
}

const credentialsMap = {
    host: fc.host,
    username: fc.username,
    user: fc.username,
    port: fc.hostPort,
    password: fc.password,
    region: fc.infrastructureRegion
};

function credentialsFromObject(credentialsObject) {
    let mappedFieldCount = 0;
    let result = {};

    Object.keys(credentialsObject).forEach((key) => {
        const field = credentialsMap[key] || fc.keyToFieldMap[key];
        if (field) {
            const sourceKey = `${field.key}_source`;
            const value = credentialsObject[key];
            if (value) {
                if (result[field.key]) {
                    const firstValueKey = result[sourceKey];
                    logging.logAndThrowError(`Found multiple options ${firstValueKey} / ${key} while preparing credentials.`);
                } else {
                    mappedFieldCount++;
                    result[field.key] = value;
                    result[sourceKey] = key;
                }
            }
        } // else - we are currently happy to have unused fields.
    });

    return mappedFieldCount ? result : null;
}

module.exports.credentialsFrom = credentialsFrom;
