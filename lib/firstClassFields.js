'use strict';

const keyToFieldMap = {};
function FirstClassField(key) {
    this.key = key;
    keyToFieldMap[key] = this;
}

FirstClassField.prototype.toString = function () {
    return this.key;
};

module.exports.host = new FirstClassField('host_(fc)');
module.exports.username = new FirstClassField('username_(fc)');
module.exports.password = new FirstClassField('password_(fc)');
module.exports.hostPort = new FirstClassField('host_port_(fc)');
module.exports.infrastructureRegion = new FirstClassField('infrastructure_region_(fc)');
module.exports.keyToFieldMap = keyToFieldMap;