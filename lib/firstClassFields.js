'use strict';

function FirstClassField(key) {
    this.key = key;
}

FirstClassField.prototype.toString = function () {
    return this.key;
};

module.exports.host = new FirstClassField('host_(fc)');
module.exports.username = new FirstClassField('username_(fc)');
module.exports.password = new FirstClassField('password_(fc)');
module.exports.hostPort = new FirstClassField('host_port_(fc)');
