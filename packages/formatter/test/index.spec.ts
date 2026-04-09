import { toError } from '@socket-mesh/core';
import assert from 'node:assert';
import { describe, it } from 'node:test';

import codec from '../src/index.js';

describe('formatter', function () {
	const str2ab = function (str: string): ArrayBuffer {
		const buf = new ArrayBuffer(str.length);
		const bufView = new Uint8Array(buf);
		for (let i = 0, strLen = str.length; i < strLen; i++) {
			bufView[i] = str.charCodeAt(i);
		}
		return buf;
	};

	describe('encode', () => {
		it('should encode an Object into a string', () => {
			const rawObject: { arr: number[], complexArr: any[], foo: number, ob: any } = {
				arr: [1, 2, 3, 4],
				complexArr: [
					{ a: 1, b: 2 },
					{ c: 3, d: 4, e: 5 },
					{ foo: 'bar' },
					['a', 'b', 'c', { nested: 'object' }]
				],
				foo: 123,
				ob: { hi: 'hello' }
			};
			rawObject.complexArr.push(rawObject.arr);

			const encoded = codec.encode(rawObject);
			const expected = JSON.stringify(rawObject);

			assert(encoded == expected, 'Encoded data did not match expected output');
		});

		it('should serialize binary Buffer objects to base64 strings', () => {
			const rawObject = {
				buff: Buffer.from('hello', 'utf8'),
				buffArr: [Buffer.from('world', 'utf8')],
				foo: 123
			};
			const encoded = codec.encode(rawObject);
			const expected = JSON.stringify({
				buff: { base64: true, data: 'aGVsbG8=' },
				buffArr: [
					{ base64: true, data: 'd29ybGQ=' }
				],
				foo: 123
			});

			assert(encoded == expected, 'Encoded data did not match expected output');
		});

		it('should serialize binary ArrayBuffer objects to base64 strings', () => {
			const rawObject = {
				buff: str2ab('hello'),
				buffArr: [str2ab('world')],
				foo: 123
			};
			const encoded = codec.encode(rawObject);
			const expected = JSON.stringify({
				buff: { base64: true, data: 'aGVsbG8=' },
				buffArr: [
					{ base64: true, data: 'd29ybGQ=' }
				],
				foo: 123
			});

			assert(encoded == expected, 'Encoded data did not match expected output');
		});

		it('should throw error if there is a circular structure - Basic', () => {
			const rawObject: { arr: any[], foo: number } = {
				arr: [],
				foo: 123
			};
			rawObject.arr.push(rawObject);

			let error: Error | null = null;
			try {
				codec.encode(rawObject);
			} catch (err) {
				error = toError(err);
			}

			assert(error != null, 'Expected an error to be thrown');
		});

		it('should throw error if there is a circular structure - Single level nesting', () => {
			const rawObject = {
				foo: {
					bar: {
						deep: {}
					},
					hello: 'world'
				}
			};
			rawObject.foo.bar.deep = rawObject.foo;

			let error: Error | null = null;

			try {
				codec.encode(rawObject);
			} catch (err) {
				error = err instanceof Error ? err : new Error(String(err));
			}
			assert(error != null, 'Expected an error to be thrown');
		});

		it('should ignore prototype properties', () => {
			(Object.prototype as any).prototypeProperty = 123;
			const rawObject = {
				foo: 123
			};
			const encoded = codec.encode(rawObject);
			const expected = '{"foo":123}';
			assert(encoded == expected, 'Encoded data did not match expected output');
			delete Object.prototype['prototypeProperty' as keyof object];
		});

		it('should ignore properties which contain functions', () => {
			const rawObject = {
				foo: 123,
				fun: function () {
					return 456;
				}
			};
			const encoded = codec.encode(rawObject);
			const expected = JSON.stringify({ foo: 123 });
			assert(encoded == expected, 'Encoded data did not match expected output');
		});

		it('should leave ping & pong messages alone', () => {
			assert.strictEqual(codec.encode('#1'), '#1');
			assert.strictEqual(codec.encode('#2'), '#2');
		});
	});
});
