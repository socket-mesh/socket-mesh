import assert from 'node:assert';
import { EventEmitter } from 'node:events';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { AsyncStreamEmitter } from '../src/index.js';

const pendingTimeoutSet = new Set<NodeJS.Timeout>();

function cancelAllPendingWaits() {
	for (const timeout of pendingTimeoutSet) {
		clearTimeout(timeout);
	}
}

function wait(duration: number): Promise<void> {
	return new Promise((resolve) => {
		const timeout = setTimeout(() => {
			pendingTimeoutSet.clear();
			resolve();
		}, duration);
		pendingTimeoutSet.add(timeout);
	});
}

describe('AsyncStreamEmitter', () => {
	describe('Functionality tests', () => {
		let streamEmitter: AsyncStreamEmitter<string>;
		let packets: string[];

		beforeEach(async () => {
			packets = [];
			streamEmitter = new AsyncStreamEmitter<string>();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
		});

		it('should support listener method which can consume emitted events and which is cleaned up after closing', async () => {
			assert.strictEqual(!!streamEmitter.emit, true);

			(async () => {
				for (let i = 0; i < 5; i++) {
					await wait(20);
					streamEmitter.emit('foo', 'hello' + i);
				}
				streamEmitter.closeListeners('foo');
			})();

			for await (const event of streamEmitter.listen('foo')) {
				packets.push(event);
			}

			const expectedEvents = [
				'hello0',
				'hello1',
				'hello2',
				'hello3',
				'hello4'
			];

			assert.strictEqual(packets.join(','), expectedEvents.join(','));
		});

		it('should stop consuming specified events after the closeAllListeners method is invoked', async () => {
			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
				}
			})();

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
			})();

			for (let i = 0; i < 5; i++) {
				await wait(20);
				streamEmitter.emit('foo', 'hello' + i);
			}

			let fooStatsList = streamEmitter.getListenerConsumerStats('foo');
			let barStatsList = streamEmitter.getListenerConsumerStats('bar');

			assert.strictEqual(fooStatsList.length, 1);
			assert.strictEqual(barStatsList.length, 1);

			streamEmitter.closeListeners();

			await wait(0);

			fooStatsList = streamEmitter.getListenerConsumerStats('foo');
			barStatsList = streamEmitter.getListenerConsumerStats('bar');

			assert.strictEqual(JSON.stringify(fooStatsList), '[]');
			assert.strictEqual(JSON.stringify(barStatsList), '[]');
		});

		it('should return a consumer stats object when the getListenerConsumerStats method is called', async () => {
			const fooConsumer = streamEmitter.listen('foo').createConsumer();

			(async () => {
				for await (const event of fooConsumer) {
					packets.push(event);
				}
			})();

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
			})();

			const fooStats = streamEmitter.getListenerConsumerStats(fooConsumer.id);

			assert.notStrictEqual(fooStats, null);
			assert.strictEqual(fooStats.backpressure, 0);
			assert.strictEqual(fooStats.stream, 'foo');
		});

		it('should return a list of consumer stats when the getListenerConsumerStatsList method is called', async () => {
			const fooConsumerA = streamEmitter.listen('foo').createConsumer();
			const fooConsumerB = streamEmitter.listen('foo').createConsumer();
			const barConsumer = streamEmitter.listen('bar').createConsumer();

			(async () => {
				for await (const event of fooConsumerA) {
					packets.push(event);
				}
			})();
			(async () => {
				for await (const event of fooConsumerB) {
					packets.push(event);
				}
			})();
			(async () => {
				for await (const event of barConsumer) {
					packets.push(event);
				}
			})();

			const fooStatsList = streamEmitter.getListenerConsumerStats('foo');
			const barStatsList = streamEmitter.getListenerConsumerStats('bar');

			assert.notStrictEqual(fooStatsList, null);
			assert.strictEqual(fooStatsList.length, 2);
			assert.strictEqual(fooStatsList[0]!.stream, 'foo');
			assert.strictEqual(fooStatsList[0]!.backpressure, 0);
			assert.strictEqual(fooStatsList[1]!.stream, 'foo');
			assert.strictEqual(fooStatsList[1]!.backpressure, 0);
			assert.notStrictEqual(barStatsList, null);
			assert.strictEqual(barStatsList.length, 1);
			assert.strictEqual(barStatsList[0]!.backpressure, 0);
			assert.strictEqual(barStatsList[0]!.stream, 'bar');
		});

		it('should return a complete list of consumer stats when the getAllListenersConsumerStatsList method is called', async () => {
			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
				}
			})();
			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
				}
			})();
			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
			})();

			const allStatsList = streamEmitter.getListenerConsumerStats();

			assert.notStrictEqual(allStatsList, null);
			assert.strictEqual(allStatsList.length, 3);
			// Check that each ID is unique.
			assert.strictEqual([...new Set(allStatsList.map(stats => stats.id))].length, 3);
		});

		it('should stop consuming on the specified listeners after the killListener method is called', async () => {
			const ended: string[] = [];

			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
				}
				ended.push('foo');
			})();

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
				ended.push('bar');
			})();

			for (let i = 0; i < 5; i++) {
				await wait(20);
				streamEmitter.emit('foo', 'hello' + i);
				streamEmitter.emit('bar', 'hi' + i);
			}
			streamEmitter.killListeners('bar');

			await wait(0);

			const allStatsList = streamEmitter.getListenerConsumerStats();

			assert.strictEqual(ended.length, 1);
			assert.strictEqual(ended[0], 'bar');
			assert.strictEqual(allStatsList.length, 1);
			assert.strictEqual(allStatsList[0]!.stream, 'foo');
		});

		it('should stop consuming on all listeners after the killAllListeners method is called', async () => {
			const ended: string[] = [];

			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
				}
				ended.push('foo');
			})();

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
				ended.push('bar');
			})();

			for (let i = 0; i < 5; i++) {
				await wait(20);
				streamEmitter.emit('foo', 'hello' + i);
				streamEmitter.emit('bar', 'hi' + i);
			}
			streamEmitter.killListeners();

			await wait(0);

			const allStatsList = streamEmitter.getListenerConsumerStats();

			assert.strictEqual(ended.length, 2);
			assert.strictEqual(ended[0], 'foo');
			assert.strictEqual(ended[1], 'bar');
			assert.strictEqual(allStatsList.length, 0);
		});

		it('should stop consuming by a specific consumer after the killListenerConsumer method is called', async () => {
			const ended: string[] = [];

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
				ended.push('bar');
			})();

			const fooConsumer = streamEmitter.listen('foo').createConsumer();

			(async () => {
				for await (const event of fooConsumer) {
					packets.push(event);
				}
				ended.push('foo');
			})();

			streamEmitter.killListeners(fooConsumer.id);

			await wait(0);

			const allStatsList = streamEmitter.getListenerConsumerStats();

			assert.strictEqual(ended.length, 1);
			assert.strictEqual(ended[0], 'foo');
			assert.strictEqual(allStatsList.length, 1);
			assert.strictEqual(allStatsList[0]!.stream, 'bar');
		});

		it('should return the backpressure of the specified event when the getListenerBackpressure method is called', async () => {
			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
					await wait(300);
				}
			})();

			for (let i = 0; i < 5; i++) {
				streamEmitter.emit('foo', 'test' + i);
			}

			assert.strictEqual(streamEmitter.getListenerBackpressure('foo'), 5);
			await wait(0);
			assert.strictEqual(streamEmitter.getListenerBackpressure('foo'), 4);
			await wait(100);
			assert.strictEqual(streamEmitter.getListenerBackpressure('foo'), 4);

			// Kill all listeners so the test dosn't hang
			streamEmitter.killListeners();
		});

		it('should return the max backpressure of all events when the getAllListenersBackpressure method is called', async () => {
			(async () => {
				for await (const event of streamEmitter.listen('foo')) {
					packets.push(event);
					await wait(300);
				}
			})();
			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
					await wait(300);
				}
			})();

			for (let i = 0; i < 5; i++) {
				streamEmitter.emit('foo', 'test' + i);
				streamEmitter.emit('bar', 'hello' + i);
				streamEmitter.emit('bar', 'hi' + i);
			}

			assert.strictEqual(streamEmitter.getListenerBackpressure(), 10);
			await wait(0);
			assert.strictEqual(streamEmitter.getListenerBackpressure(), 9);

			// Kill all listeners so the test dosn't hang
			streamEmitter.killListeners();
		});

		it('should return the backpressure of the specified consumer when getListenerConsumerBackpressure method is called', async () => {
			const fooConsumer = streamEmitter.listen('foo').createConsumer();
			(async () => {
				for await (const event of fooConsumer) {
					packets.push(event);
					await wait(300);
				}
			})();
			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
					await wait(300);
				}
			})();

			for (let i = 0; i < 5; i++) {
				streamEmitter.emit('foo', 'test' + i);
				streamEmitter.emit('bar', 'hello' + i);
			}

			assert.strictEqual(streamEmitter.getListenerBackpressure(fooConsumer.id), 5);
			await wait(0);
			assert.strictEqual(streamEmitter.getListenerBackpressure(fooConsumer.id), 4);

			// Kill all listeners so the test dosn't hang
			streamEmitter.killListeners();
		});

		it('should return the correct boolean when hasListenerConsumer method is called', async () => {
			const fooConsumer = streamEmitter.listen('foo').createConsumer();
			(async () => {
				for await (const event of fooConsumer) {
					packets.push(event);
					await wait(300);
				}
			})();
			assert.strictEqual(streamEmitter.hasListenerConsumer('foo', fooConsumer.id), true);
			assert.strictEqual(streamEmitter.hasListenerConsumer('bar', fooConsumer.id), false);
			assert.strictEqual(streamEmitter.hasListenerConsumer('foo', 9), false);
		});

		it('should return the correct boolean when hasAnyListenerConsumer method is called', async () => {
			const fooConsumer = streamEmitter.listen('foo').createConsumer();
			(async () => {
				for await (const event of fooConsumer) {
					packets.push(event);
					await wait(300);
				}
			})();
			assert.strictEqual(streamEmitter.hasListenerConsumer(fooConsumer.id), true);
			assert.strictEqual(streamEmitter.hasListenerConsumer(9), false);
		});

		it('should stop consuming processing a specific event after a listener is removed with the removeListener method', async () => {
			const ended: string[] = [];

			(async () => {
				for await (const event of streamEmitter.listen('bar')) {
					packets.push(event);
				}
				ended.push('bar');
			})();

			for (let i = 0; i < 5; i++) {
				streamEmitter.emit('bar', 'a' + i);
			}

			streamEmitter.removeListener('bar');
			await wait(0);
			streamEmitter.listen('bar').once();

			for (let i = 0; i < 5; i++) {
				streamEmitter.emit('bar', 'b' + i);
			}

			await wait(0);

			assert.notStrictEqual(packets, null);
			assert.strictEqual(packets.length, 5);
			assert.strictEqual(packets[0], 'a0');
			assert.strictEqual(packets[4], 'a4');
			assert.strictEqual(ended.length, 0);
		});
	});

	describe('From Tests', () => {
		let emitter: EventEmitter;
		let streamEmitter: AsyncStreamEmitter<string>;

		beforeEach(async () => {
			emitter = new EventEmitter();
			streamEmitter = AsyncStreamEmitter.from<string>(emitter);
		});

		afterEach(async () => {
			cancelAllPendingWaits();
		});

		it('should add AsyncStreamEmitter methods to the EventEmitter instance', async () => {
			assert.strictEqual(!!streamEmitter.emit, true);
			assert.strictEqual(!!streamEmitter.listen, true);
			assert.strictEqual(!!streamEmitter.closeListeners, true);
		});

		it('should support AsyncStreamEmitter functionality', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					streamEmitter.emit('foo', 'hello' + i);
				}
				streamEmitter.closeListeners('foo');
			})();

			let receivedData: string[] = [];

			for await (const data of streamEmitter.listen('foo')) {
				receivedData.push(data);
			}

			assert.strictEqual(receivedData.length, 10);
			assert.strictEqual(receivedData[0], 'hello0');
			assert.strictEqual(receivedData[9], 'hello9');

			(async () => {
				for (let i = 0; i < 20; i++) {
					await wait(10);
					streamEmitter.emit('bar', 'hi' + i);
				}
				streamEmitter.closeListeners();
			})();

			receivedData = [];

			for await (const data of streamEmitter.listen('bar')) {
				receivedData.push(data);
			}

			assert.strictEqual(receivedData.length, 20);
			assert.strictEqual(receivedData[0], 'hi0');
			assert.strictEqual(receivedData[19], 'hi19');

			(async () => {
				await wait(10);
				streamEmitter.emit('test', 'abc');
			})();

			const data = await streamEmitter.listen('test').once();
			assert.strictEqual(data, 'abc');
		});

		it('should retain the original emitter functionality', () => {
			let b = '';
			let c = '';

			emitter.on('a', (b1, c1) => {
				b = b1;
				c = c1;
			});

			emitter.emit('a', 'b', 'c');

			assert.strictEqual(b, 'b');
			assert.strictEqual(c, 'c');
		});
	});
});
