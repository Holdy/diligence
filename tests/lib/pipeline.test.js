const pipeline = require('../../lib/pipeline');

describe('pipeline.fieldBatcher()', () => {
    test('should batch on int id', async () => {

        const outputs = [];
        const sut = pipeline.from(
            pipeline.fieldBatcher('id'),
            pipeline.filterMap((item) => {
                outputs.push(item);
            })
        );
   
        await sut.processEachAsync([
            {id:1, data:'a'},
            {id:2, data:'b'},
            {id:2, data:'c'},
            {id:2, data:'d'},
            {id:3, data:'e'},
        ]);

        expect(outputs.length).toBe(3);
        expect(outputs[0].length).toBe(1);
        expect(outputs[1].length).toBe(3);
        expect(outputs[2].length).toBe(1);
    });

    test('final flush with no pending batch should not cause flush (covers if statement)', async () => {

        const outputs = [];
        const sut = pipeline.from(
            pipeline.fieldBatcher('id'),
            pipeline.filterMap((item) => {
                outputs.push(item);
            })
        );
   
        await sut.processEachAsync([]);

        expect(outputs.length).toBe(0);
    });

});