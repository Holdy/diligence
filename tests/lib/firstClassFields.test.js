'use strict';

const sut = require('../../lib/firstClassFields');

test('all fields should be usable as keys', () => {
    Object.keys(sut).forEach((exportKey) => {
        expectToStringToMatchKeyField(sut[exportKey]);
    });
});

test('specific field should be usable as key', () => {
    expectToStringToMatchKeyField(sut.hostPort);
});

function expectToStringToMatchKeyField(field) {
    const holder = {};
    holder[field.key] = 'the_data';

    const retrievedData = holder[field];

    expect(retrievedData).toBe('the_data');
}
