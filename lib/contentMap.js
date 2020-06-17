'use strict';

const map = {};

function set(content, metaData) {
    map[content] = metaData;
}

function get(content) {
    return map[content];
}

module.exports.get = get;
module.exports.set = set;
