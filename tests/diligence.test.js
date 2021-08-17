const sut = require('../diligence');

describe('main-module', () => {

    describe('sumField', () => {
        
        test('should cope with empty list', () => {
            const input = [];

            const result = sut.sumField(input, 'total');

            expect(result).toBe(0);
        });

        test('should cope with missing data', () => {
            const input = [
                {name: 'a'},
                {name:'b', total: null},
                {name:'c', total: 1},
                {name:'d', total: 0.5},
            ];

            const result = sut.sumField(input, 'total');

            expect(result).toBe(1.5);
        });

    });


});