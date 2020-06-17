'use strict';

const contentMap = require('../contentMap');
const logging = require('../logging');
const pipeline = require('../pipeline');
const ensure = require('../ensure');

class PostgresAbstraction {
    constructor(pool) {
        this.pool = pool;
    }

    async performUpdateAsync(sql_template, parameters) {
        const metaData = contentMap.get(sql_template);
        const sqlDescriptor = metaData && metaData.fileName ? metaData.fileName : 'sql-update-statement';

        logging.logInfo(`performUpdateAsync - Executing SQL update - ${sqlDescriptor}`);
        const executionData = mergeTemplate(sql_template, parameters, sqlDescriptor);
        try {
            const concreteResult = await this.pool.query(executionData.sql, executionData.parameters);
            const rowCount = concreteResult.rowCount;
            const verb = concreteResult.command && concreteResult.command === 'INSERT' ? 'inserted' : 'affected';
            logging.logInfo(`performUpdateAsync - Completed ${sqlDescriptor} - ${rowCount} rows ${verb}`);
        } catch (err) {
            logging.logAndThrowError(`performUpdateAsync - Error while running ${sqlDescriptor}`, err);
        }
    }

    async performSelectAsync(sql_template, parameters) {
        const metaData = contentMap.get(sql_template);
        const sqlDescriptor = metaData && metaData.fileName ? metaData.fileName : 'sql-select-statement';

        logging.logInfo(`performSelectAsync - Executing SQL select - ${sqlDescriptor}`);
        const executionData = mergeTemplate(sql_template, parameters, sqlDescriptor);
        try {
            const concreteResult = await this.pool.query(executionData.sql, executionData.parameters);
            const rowCount = concreteResult.rowCount;
            const verb = concreteResult.command && concreteResult.command === 'SELECT' ? 'selected' : 'affected';
            logging.logInfo(`performSelectAsync - Completed ${sqlDescriptor} - ${rowCount} rows ${verb}`);

            const abstractedResult = {
                rows: concreteResult.rows,
            };
            return abstractedResult;
        } catch (err) {
            logging.logAndThrowError(`performSelectAsync - Error while running ${sqlDescriptor}`, err);
        }
    }
}

function mergeTemplate(sqlStatementTemplate, parameters, sqlStatementDescriptor) {
    const parameterList = [];
    let progressiveTemplate = sqlStatementTemplate;

    while (progressiveTemplate.indexOf("'{{{") != -1) {
        const startIndex = progressiveTemplate.indexOf("'{{{");
        const endIndex = progressiveTemplate.indexOf("}}}'", startIndex);
        if (endIndex == -1) {
            throw new Error(`Failed to to parse sql parameter starting in ${sqlStatementDescriptor}`);
        }
        const placeholderKey = progressiveTemplate.substring(startIndex + 4, endIndex);
        const placeholderValue = ensure.defined(
            parameters[placeholderKey],
            `Template ${sqlStatementDescriptor} called for parameter ${placeholderKey} which was not available`
        );
        parameterList.push(placeholderValue);
        const placeholderName = `$${parameterList.length}`;

        progressiveTemplate = progressiveTemplate.substring(0, startIndex) + placeholderName + progressiveTemplate.substring(endIndex + 4);
    }

    return {
        sql: progressiveTemplate,
        parameters: parameterList,
    };
}

module.exports = PostgresAbstraction;
