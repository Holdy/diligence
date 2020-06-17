'use strict';

const logging = require('../../lib/logging');

const sut = require('../../lib/json');

const arbitraryContext = { some: 'context-data' };

describe('parser()', () => {

  test('should behave like a good pipeline component - happy path', async () => {
      const component = sut.parser();
      component.emitAsync = jest.fn();
      component.emitCompleteAsync = jest.fn();
      logging.logInfo = jest.fn();

      await component.processAsync('{"my": "data"}', {descriptor:'serialised-json'}, arbitraryContext);
      await component.processCompleteAsync(arbitraryContext);

      expect(component.emitAsync.mock.calls.length).toBe(1, 'expected stringified data to be emitted');
      expect(component.emitAsync.mock.calls[0][0]).toStrictEqual({'my': 'data'});
      expect(component.emitAsync.mock.calls[0][1]).toStrictEqual({descriptor: 'json from serialised-json'});
      expect(component.emitAsync.mock.calls[0][2]).toStrictEqual(arbitraryContext); 
   
      expect(component.emitCompleteAsync.mock.calls.length).toBe(1, 'expected complete to be emitted');
      expect(component.emitCompleteAsync.mock.calls[0][0]).toStrictEqual(arbitraryContext);
      expect(logging.logInfo.mock.calls.length).toBe(1);
      expect(logging.logInfo.mock.calls[0][0]).toBe('p: json.parser - successfully parsed 1 items, Errored on 0 items');
  });

  test('should behave like a good pipeline component - error path', async () => {
    const component = sut.parser();
    component.emitAsync = jest.fn();
    component.emitCompleteAsync = jest.fn();
    logging.logInfo = jest.fn();

    try {
      await component.processAsync('{"my": "dat---bzzzat-oops...', {descriptor:'serialised-json'}, arbitraryContext);
    } catch (err) {
      expect(err.message).toBe('Failed to parse serialised-json');
    }
    await component.processCompleteAsync(arbitraryContext);

    expect(component.emitAsync.mock.calls.length).toBe(0, 'expected no data to be emitted');

    expect(component.emitCompleteAsync.mock.calls.length).toBe(1, 'expected complete to be emitted');
    expect(component.emitCompleteAsync.mock.calls[0][0]).toStrictEqual(arbitraryContext);

    expect(logging.logInfo.mock.calls.length).toBe(1);
    expect(logging.logInfo.mock.calls[0][0]).toBe('p: json.parser - successfully parsed 0 items, Errored on 1 items');
    expect.assertions(6);
});

});

describe('prettyPrinter', () => {

    test('should behave like a good pipeline component - happy path', async () => {
        const component = sut.prettyPrinter();
        component.emitAsync = jest.fn();
        component.emitCompleteAsync = jest.fn();

        await component.processAsync({ my: 'data' }, null, null);
        await component.processCompleteAsync(arbitraryContext);

        expect(component.emitAsync.mock.calls.length).toBe(1, 'expected stringified data to be emitted');
        expect(component.emitAsync.mock.calls[0][0]).toBe('{\n   "my": "data"\n}');
        expect(component.emitCompleteAsync.mock.calls.length).toBe(1, 'expected complete to be emitted');
        expect(component.emitCompleteAsync.mock.calls[0][0]).toStrictEqual(arbitraryContext);
    });

    test('should behave like a good pipeline component - failure path', async () => {
        const component = sut.prettyPrinter();
        component.emitAsync = jest.fn();
        logging.logError = jest.fn();
        expect.assertions(4);

        // Passing a function will cause a serialisation failure
        try {
            await component.processAsync(console.log, null, null);
        } catch (err) {
            expect(err.message).toBe('Failed to stringify [unnamed-item]');
        }

        expect(component.emitAsync.mock.calls.length).toBe(0, 'expected no data to be emitted');
        expect(logging.logError.mock.calls.length).toBe(1, 'expected log-error to be called');
        expect(logging.logError.mock.calls[0][0]).toBe('Failed to stringify [unnamed-item]');
    });

    test('should use metadata.description when present', async () => {
        const component = sut.prettyPrinter();
        component.emitAsync = jest.fn();
        expect.assertions(1);

        // Passing a function will cause a serialisation failure
        try {
            await component.processAsync(console.log, {descriptor:'my-thing'}, null);
        } catch (err) {
            expect(err.message).toBe('Failed to stringify [my-thing]');
        }

    });
});

describe('coalesce()', () => {
    test('coalesce should return null for missing field', () => {
        const data = {};

        const result = sut.coalesce(data, ['missing-field']);

        expect(result).toBeNull();
    });

    test('coalesce should return a present string', () => {
        const data = { somekey: 'zero' };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe('zero');
    });

    test('coalesce should return a present non zero integer', () => {
        const data = { somekey: 1 };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe(1);
    });

    test('coalesce should return a present zero integer', () => {
        const data = { somekey: 0 };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe(0);
    });

    test('coalesce should return a present true', () => {
        const data = { somekey: true };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe(true);
    });

    test('coalesce should return a present false', () => {
        const data = { somekey: false };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe(false);
    });

    test('coalesce should return a present empty string', () => {
        const data = { somekey: '' };

        const result = sut.coalesce(data, ['somekey']);

        expect(result).toBe('');
    });

    test('coalesce should process all keys to find a valid path', () => {
        const data = { you_got_it: 'congrats' };

        const result = sut.coalesce(data, ['not-this'], ['hardly'], ['you_got_it']);

        expect(result).toBe('congrats');
    });

    test('coalesce should recover from a failed path and succeed', () => {
        const data = { step1: { step2: 'yummy-data' } };

        const result = sut.coalesce(data, ['step1', 'oops'], ['step1', 'step2']);

        expect(result).toBe('yummy-data');
    });

    test('coalesce should return null for failed paths', () => {
        const data = {};

        const result = sut.coalesce(data, ['step1', 'oops'], ['step1', 'step2']);

        expect(result).toBeNull();
    });

    test('coalesce should return null if the input is null', () => {
        const data = null;

        const result = sut.coalesce(data, ['step1', 'oops'], ['step1', 'step2']);

        expect(result).toBeNull();
    });
});
