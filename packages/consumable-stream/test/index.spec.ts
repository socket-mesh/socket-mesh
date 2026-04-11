import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { ConsumableStream } from '../src/index.js';

const pendingTimeoutSet = new Set<NodeJS.Timeout>();

function cancelAllPendingWaits() {
	for (const timeout of pendingTimeoutSet) {
		clearTimeout(timeout);
	}
}

function wait(duration: number): Promise<void> {
	return new Promise((resolve) => {
		const timeout = setTimeout(() => {
			pendingTimeoutSet.delete(timeout);
			resolve();
		}, duration);
		pendingTimeoutSet.add(timeout);
	});
}

class ConsumableStreamSubclass<T> extends ConsumableStream<T> {
	private _dataPromiseList: Promise<T>[];

	constructor(dataPromiseList: Promise<T>[]) {
		super();
		this._dataPromiseList = dataPromiseList;
	}

	async* createConsumer() {
		while (this._dataPromiseList.length) {
			const result = await this._dataPromiseList[this._dataPromiseList.length - 1]!;
			yield result;
		}
	}
}

describe('ConsumableStream', () => {
	let stream: ConsumableStreamSubclass<number>;

	beforeEach(async () => {
		const streamData = [...Array(10).keys()]
			.map(async (value, index) => {
				await wait(20 * (index + 1));
				streamData.pop();
				return value;
			})
			.reverse();

		stream = new ConsumableStreamSubclass(streamData);
	});

	afterEach(async () => {
		cancelAllPendingWaits();
	});

	it('should receive packets asynchronously', async () => {
		const receivedPackets: number[] = [];
		for await (const packet of stream) {
			receivedPackets.push(packet);
		}
		assert.strictEqual(receivedPackets.length, 10);
		assert.strictEqual(receivedPackets[0], 0);
		assert.strictEqual(receivedPackets[1], 1);
		assert.strictEqual(receivedPackets[9], 9);
	});

	it('should receive packets asynchronously from multiple concurrent for-await-of loops', async () => {
		const receivedPacketsA: number[] = [];
		const receivedPacketsB: number[] = [];

		await Promise.all([
			(async () => {
				for await (const packet of stream) {
					receivedPacketsA.push(packet);
				}
			})(),
			(async () => {
				for await (const packet of stream) {
					receivedPacketsB.push(packet);
				}
			})()
		]);

		assert.strictEqual(receivedPacketsA.length, 10);
		assert.strictEqual(receivedPacketsA[0], 0);
		assert.strictEqual(receivedPacketsA[1], 1);
		assert.strictEqual(receivedPacketsA[9], 9);
	});

	it('should receive next packet asynchronously when once() method is used', async () => {
		let nextPacket = await stream.once();
		assert.strictEqual(nextPacket, 0);

		nextPacket = await stream.once();
		assert.strictEqual(nextPacket, 1);

		nextPacket = await stream.once();
		assert.strictEqual(nextPacket, 2);
	});
});
