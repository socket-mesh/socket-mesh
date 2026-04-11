/* eslint-disable no-empty */
// There seems to be an issue when the using typescript with a target older than ES2020.
// It does not call the consumer exit function when for await loops exit.
// Because of this I am leaving the js version of the test here.

import assert from 'node:assert';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { WritableConsumableStream } from '../src/index.js';
import { WritableStreamConsumer } from '../src/writable-stream-consumer.js';

const pendingTimeoutSet = new Set<NodeJS.Timeout>();

function cancelAllPendingWaits() {
	for (const timeout of pendingTimeoutSet) {
		clearTimeout(timeout);
	}
}

function wait(duration: number) {
	return new Promise<void>((resolve) => {
		const timeout = setTimeout(() => {
			pendingTimeoutSet.delete(timeout);
			resolve();
		}, duration);
		pendingTimeoutSet.add(timeout);
	});
}

describe('WritableConsumableStream', () => {
	let stream: WritableConsumableStream<string>;

	describe('for-await-of loop', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream<string>();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should receive packets asynchronously', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				receivedPackets.push(packet);
			}
			assert.strictEqual(receivedPackets.length, 10);
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('a' + i);
					stream.write('b' + i);
					stream.write('c' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				receivedPackets.push(packet);
			}
			assert.strictEqual(receivedPackets.length, 30);
			assert.strictEqual(receivedPackets[0], 'a0');
			assert.strictEqual(receivedPackets[1], 'b0');
			assert.strictEqual(receivedPackets[2], 'c0');
			assert.strictEqual(receivedPackets[3], 'a1');
			assert.strictEqual(receivedPackets[4], 'b1');
			assert.strictEqual(receivedPackets[5], 'c1');
			assert.strictEqual(receivedPackets[29], 'c9');
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should receive packets if stream is written to from inside a consuming for-await-of loop', async () => {
			(async () => {
				for (let i = 0; i < 3; i++) {
					await wait(10);
					stream.write('a' + i);
				}
			})();

			let count = 0;
			const receivedPackets: string[] = [];

			for await (const packet of stream) {
				receivedPackets.push(packet);
				stream.write('nested' + count);
				if (++count > 10) {
					break;
				}
			}

			assert.strictEqual(receivedPackets[0], 'a0');
			assert.strictEqual(receivedPackets.some(message => message === 'nested0'), true);
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should only consume messages which were written after the consumer was created', async () => {
			stream.write('one');
			stream.write('two');

			const receivedPackets: string[] = [];

			const doneConsumingPromise = (async () => {
				for await (const packet of stream) {
					receivedPackets.push(packet);
				}
			})();

			stream.write('three');
			stream.write('four');
			stream.write('five');
			stream.close();

			await doneConsumingPromise;

			assert.strictEqual(receivedPackets.length, 3);
			assert.strictEqual(receivedPackets[0], 'three');
			assert.strictEqual(receivedPackets[1], 'four');
			assert.strictEqual(receivedPackets[2], 'five');
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should not miss packets if it awaits inside a for-await-of loop', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(2);
					stream.write('a' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				receivedPackets.push(packet);
				await wait(50);
			}

			assert.strictEqual(receivedPackets.length, 10);
			for (let i = 0; i < 10; i++) {
				assert.strictEqual(receivedPackets[i], 'a' + i);
			}
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should not miss packets if it awaits inside two concurrent for-await-of loops', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('a' + i);
				}
				stream.close();
			})();

			const receivedPacketsA: string[] = [];
			const receivedPacketsB: string[] = [];

			await Promise.all([
				(async () => {
					for await (const packet of stream) {
						receivedPacketsA.push(packet);
						await wait(5);
					}
				})(),
				(async () => {
					for await (const packet of stream) {
						receivedPacketsB.push(packet);
						await wait(50);
					}
				})()
			]);

			assert.strictEqual(receivedPacketsA.length, 10);
			for (let i = 0; i < 10; i++) {
				assert.strictEqual(receivedPacketsA[i], 'a' + i);
			}

			assert.strictEqual(receivedPacketsB.length, 10);
			for (let i = 0; i < 10; i++) {
				assert.strictEqual(receivedPacketsB[i], 'a' + i);
			}
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to resume consumption after the stream has been closed', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('a' + i);
				}
				stream.close();
			})();

			const receivedPacketsA: string[] = [];
			for await (const packet of stream) {
				receivedPacketsA.push(packet);
			}

			assert.strictEqual(receivedPacketsA.length, 10);

			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('b' + i);
				}
				stream.close();
			})();

			const receivedPacketsB: string[] = [];
			for await (const packet of stream) {
				receivedPacketsB.push(packet);
			}

			assert.strictEqual(receivedPacketsB.length, 10);
			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to resume consumption of messages written within the same stack frame after the stream has been closed', async () => {
			stream.write('one');
			stream.write('two');

			const receivedPackets: string[] = [];

			const doneConsumingPromiseA = (async () => {
				for await (const packet of stream) {
					receivedPackets.push(packet);
				}
			})();

			stream.write('three');
			stream.write('four');
			stream.write('five');
			stream.close();

			await doneConsumingPromiseA;

			const doneConsumingPromiseB = (async () => {
				for await (const packet of stream) {
					receivedPackets.push(packet);
				}
			})();

			stream.write('six');
			stream.write('seven');
			stream.close();

			await doneConsumingPromiseB;

			assert.strictEqual(receivedPackets.length, 5);
			assert.strictEqual(receivedPackets[0], 'three');
			assert.strictEqual(receivedPackets[1], 'four');
			assert.strictEqual(receivedPackets[2], 'five');
			assert.strictEqual(receivedPackets[3], 'six');
			assert.strictEqual(receivedPackets[4], 'seven');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to optionally timeout the consumer iterator when write delay is consistent', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(30);
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const consumable = stream.createConsumer(20);
			let error: Error | undefined;

			try {
				for await (const packet of consumable) {
					receivedPackets.push(packet);
				}
			} catch (err) {
				error = err as Error;
			}

			const consumerStatsList = stream.getConsumerStats();
			assert.strictEqual(consumerStatsList.length, 0);

			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');
			assert.strictEqual(receivedPackets.length, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to optionally timeout the consumer iterator when write delay is inconsistent', async () => {
			(async () => {
				await wait(10);
				stream.write('hello0');
				await wait(10);
				stream.write('hello1');
				await wait(10);
				stream.write('hello2');
				await wait(30);
				stream.write('hello3');
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const consumable = stream.createConsumer(20);
			let error: Error | null = null;

			try {
				for await (const packet of consumable) {
					receivedPackets.push(packet);
				}
			} catch (err) {
				error = err as Error;
			}

			const consumerStatsList = stream.getConsumerStats();
			assert.strictEqual(consumerStatsList.length, 0);

			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');
			assert.strictEqual(receivedPackets.length, 3);
			assert.strictEqual(receivedPackets[0], 'hello0');
			assert.strictEqual(receivedPackets[2], 'hello2');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to optionally timeout the consumer iterator even if steam is not explicitly closed', async () => {
			(async () => {
				await wait(10);
				stream.write('hello0');
				await wait(10);
				stream.write('hello1');
				await wait(30);
				stream.write('hello2');
			})();

			const receivedPackets: string[] = [];
			const consumable = stream.createConsumer(20);
			let error: Error | undefined;
			try {
				for await (const packet of consumable) {
					receivedPackets.push(packet);
				}
			} catch (err) {
				error = err as Error;
			}

			const consumerStatsList = stream.getConsumerStats();
			assert.strictEqual(consumerStatsList.length, 0);

			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');
			assert.strictEqual(receivedPackets.length, 2);
			assert.strictEqual(receivedPackets[0], 'hello0');
			assert.strictEqual(receivedPackets[1], 'hello1');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to resume consumption immediately after stream is closed unless a condition is met', async () => {
			let shouldResume = true;

			(async () => {
				for (let i = 0; i < 5; i++) {
					await wait(10);
					stream.write('hello' + i);
				}
				// Consumer should be able to resume without missing any messages.
				stream.close();
				stream.write('world0');
				for (let i = 1; i < 5; i++) {
					await wait(10);
					stream.write('world' + i);
				}
				shouldResume = false;
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const consumable = stream.createConsumer();

			while (true) {
				for await (const data of consumable) {
					receivedPackets.push(data);
				}
				// eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
				if (!shouldResume) break;
			}

			assert.strictEqual(receivedPackets.length, 10);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to close stream with custom data', async () => {
			(async () => {
				for (let i = 0; i < 5; i++) {
					await wait(10);
					stream.write('hello' + i);
				}
				stream.close('done123');
			})();

			const receivedPackets: string[] = [];
			let receivedEndPacket: string | undefined;
			const consumer = stream.createConsumer();

			while (true) {
				const packet = await consumer.next();
				if (packet.done) {
					receivedEndPacket = packet.value;
					break;
				}
				receivedPackets.push(packet.value);
			}

			assert.strictEqual(receivedPackets.length, 5);
			assert.strictEqual(receivedEndPacket, 'done123');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});
	});

	describe('kill', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should stop consumer immediately when stream is killed', async () => {
			let backpressureBeforeKill;
			let backpressureAfterKill;
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				backpressureBeforeKill = stream.getBackpressure();
				stream.kill();
				backpressureAfterKill = stream.getBackpressure();
			})();

			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				await wait(50);
				receivedPackets.push(packet);
			}

			const backpressureAfterConsume = stream.getBackpressure();

			assert.strictEqual(backpressureBeforeKill, 10);
			assert.strictEqual(backpressureAfterKill, 0);
			assert.strictEqual(backpressureAfterConsume, 0);
			assert.strictEqual(receivedPackets.length, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should cancel timeout when stream is killed', async () => {
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				stream.kill();
			})();

			let error: Error | undefined;

			try {
				await Promise.race([
					stream.once(200), // This should throw an error early.
					wait(100)
				]);
			} catch (err) {
				error = err as Error;
			}

			assert.strictEqual(error!.name, 'TimeoutError');

			const backpressure = stream.getBackpressure();
			assert.strictEqual(backpressure, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to restart a killed stream', async () => {
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				await wait(10);
				stream.kill();

				await wait(70);

				for (let i = 0; i < 10; i++) {
					stream.write('world' + i);
				}
				await wait(10);
				stream.close();
			})();

			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				await wait(50);
				receivedPackets.push(packet);
			}

			for await (const packet of stream) {
				await wait(50);
				receivedPackets.push(packet);
			}

			const backpressure = stream.getBackpressure();
			assert.strictEqual(backpressure, 0);

			assert.strictEqual(receivedPackets.length, 11);
			assert.strictEqual(receivedPackets[0], 'hello0');
			assert.strictEqual(receivedPackets[1], 'world0');
			assert.strictEqual(receivedPackets[10], 'world9');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to start writing to a killed stream immediately', async () => {
			(async () => {
				await wait(10);
				stream.kill();
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const consumer = stream.createConsumer();

			const receivedPackets: IteratorResult<string>[] = [];
			while (true) {
				const packet = await consumer.next();
				if (packet.done) break;
				receivedPackets.push(packet);
			}
			while (true) {
				const packet = await consumer.next();
				if (packet.done) break;
				receivedPackets.push(packet);
			}
			assert.strictEqual(receivedPackets.length, 10);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should set consumer.isAlive to false if stream is killed', async () => { // TODO 22
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const consumer = stream.createConsumer();
			assert.strictEqual(consumer.isAlive, true);
			stream.kill();
			assert.strictEqual(consumer.isAlive, false);

			const receivedPackets: IteratorResult<string>[] = [];
			while (true) {
				const packet = await consumer.next();
				if (packet.done) break;
				receivedPackets.push(packet);
			}
			assert.strictEqual(receivedPackets.length, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should pass kill data to consumer when stream is killed if using consumer', async () => {
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				stream.kill('12345');
			})();

			const consumer = stream.createConsumer();
			const receivedPackets: IteratorResult<string>[] = [];
			while (true) {
				const packet = await consumer.next();
				await wait(50);
				receivedPackets.push(packet);
				if (packet.done) {
					break;
				}
			}
			assert.strictEqual(receivedPackets.length, 1);
			assert.strictEqual(receivedPackets[0]!.value, '12345');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should stop consumer at the end of the current iteration when stream is killed and iteration has already started', async () => {
			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				await wait(10);
				stream.kill();
			})();
			const receivedPackets: string[] = [];
			for await (const packet of stream) {
				await wait(50);
				receivedPackets.push(packet);
			}
			assert.strictEqual(receivedPackets.length, 1);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should stop all consumers immediately', async () => {
			let isWriting = true;

			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(40);
					// eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
					if (!isWriting) return;
					stream.write('a' + i);
				}
			})();

			(async () => {
				await wait(240);
				stream.kill();
				isWriting = false;
			})();

			const receivedPacketsA: string[] = [];
			const receivedPacketsB: string[] = [];

			await Promise.all([
				(async () => {
					for await (const packet of stream) {
						receivedPacketsA.push(packet);
					}
				})(),
				(async () => {
					for await (const packet of stream) {
						receivedPacketsB.push(packet);
						await wait(300);
					}
				})()
			]);

			assert.strictEqual(receivedPacketsA.length, 5);
			assert.strictEqual(receivedPacketsB.length, 1);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should stop consumers which have not started iterating', async () => {
			const consumer = stream.createConsumer();

			for (let i = 0; i < 10; i++) {
				stream.write('hello' + i);
			}

			stream.kill('end');

			await wait(10);

			assert.strictEqual(consumer.getBackpressure(), 0);
		});
	});

	describe('backpressure', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should track backpressure correctly when consuming stream', async () => {
			await Promise.all([
				(async () => {
					let consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 0);

					await wait(10);

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 0);
					assert.strictEqual(consumerStats[0]!.id, 1);

					assert.strictEqual(stream.hasConsumer(1), true);
					assert.strictEqual(stream.hasConsumer(2), false);

					stream.write('a0');

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 1);

					await wait(10);

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 0);

					stream.write('a1');

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 1);

					await wait(10);
					stream.write('a2');
					await wait(10);
					stream.write('a3');
					await wait(10);
					stream.write('a4');

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 4);

					stream.close();

					consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 1);
					assert.strictEqual(consumerStats[0]!.backpressure, 5);
				})(),
				(async () => {
					let expectedPressure = 6;
					for await (const _ of stream) {
						expectedPressure--;
						await wait(70);
						const consumerStats = stream.getConsumerStats();
						assert.strictEqual(consumerStats.length, 1);
						assert.strictEqual(consumerStats[0]!.backpressure, expectedPressure);
					}
					const consumerStats = stream.getConsumerStats();
					assert.strictEqual(consumerStats.length, 0);
				})()
			]);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should track backpressure correctly when consuming stream with a consumer', async () => {
			await Promise.all([
				(async () => {
					for (let i = 0; i < 10; i++) {
						await wait(10);
						stream.write('a' + i);
						const consumerStats = stream.getConsumerStats();
						assert.strictEqual(consumerStats.length, 1);
						assert.strictEqual(consumerStats[0]!.backpressure, i + 1);
					}
					stream.close();
				})(),
				(async () => {
					const iter = stream.createConsumer();
					assert.strictEqual(iter.id, 1);

					await wait(20);
					let expectedPressure = 11;
					while (true) {
						expectedPressure--;
						await wait(140);
						const data = await iter.next();
						const consumerStats = stream.getConsumerStats();

						if (data.done) {
							assert.strictEqual(consumerStats.length, 0);
							break;
						}
						assert.strictEqual(consumerStats.length, 1);
						assert.strictEqual(consumerStats[0]!.backpressure, expectedPressure);
					}
				})()
			]);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should track backpressure correctly when writing to and consuming stream intermittently with multiple consumers', async () => {
			const iterA = stream.createConsumer();
			assert.strictEqual(iterA.id, 1);

			let consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 1);
			assert.strictEqual(consumerStats[0]!.backpressure, 0);

			await wait(10);

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 1);
			assert.strictEqual(consumerStats[0]!.backpressure, 0);

			stream.write('a0');

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 1);
			assert.strictEqual(consumerStats[0]!.backpressure, 1);

			stream.write('a1');
			await wait(10);
			stream.write('a2');
			await wait(10);

			const iterB = stream.createConsumer();
			assert.strictEqual(iterB.id, 2);

			stream.write('a3');
			await wait(10);
			stream.write('a4');

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 5);

			assert.strictEqual(stream.getBackpressure(), 5);

			assert.strictEqual(iterA.getBackpressure(), 5);
			assert.strictEqual(stream.getBackpressure(1), 5);
			assert.strictEqual(iterB.getBackpressure(), 2);
			assert.strictEqual(stream.getBackpressure(2), 2);

			await iterA.next();

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 4);

			await iterA.next();
			await iterA.next();

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 2);

			stream.write('a5');
			stream.write('a6');
			stream.write('a7');

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 5);
			assert.strictEqual(stream.getBackpressure(2), 5);

			stream.close();

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 6);

			assert.strictEqual(stream.getBackpressure(), 6);

			await iterA.next();
			await iterA.next();
			await wait(10);
			await iterA.next();
			await iterA.next();
			await iterA.next();

			assert.strictEqual(stream.getBackpressure(2), 6);

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 2);
			assert.strictEqual(consumerStats[0]!.backpressure, 1);

			await iterB.next();
			await iterB.next();
			await iterB.next();
			await iterB.next();
			await iterB.next();

			assert.strictEqual(stream.getBackpressure(2), 1);

			const iterDataB = await iterB.next();

			assert.strictEqual(stream.getBackpressure(2), 0);

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 1);
			assert.strictEqual(consumerStats[0]!.backpressure, 1);

			assert.strictEqual(stream.getBackpressure(), 1);

			const iterDataA = await iterA.next();

			consumerStats = stream.getConsumerStats();
			assert.strictEqual(consumerStats.length, 0);
			assert.strictEqual(iterDataA.done, true);
			assert.strictEqual(iterDataB.done, true);

			assert.strictEqual(iterA.getBackpressure(), 0);
			assert.strictEqual(iterB.getBackpressure(), 0);

			assert.strictEqual(stream.getBackpressure(), 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should reset backpressure after invoking consumer.return()', async () => {
			const consumer = stream.createConsumer();

			for (let i = 0; i < 10; i++) {
				stream.write('hello' + i);
			}
			stream.close('end');

			await wait(10);
			consumer.return();

			assert.strictEqual(stream.getConsumerStats().length, 0);
			assert.strictEqual(consumer.getBackpressure(), 0);

			for (let i = 0; i < 10; i++) {
				stream.write('hi' + i);
			}
			stream.close('end');

			await wait(10);

			assert.strictEqual(stream.getConsumerStats().length, 0);
			assert.strictEqual(consumer.getBackpressure(), 0);

			consumer.return();

			assert.strictEqual(stream.getConsumerStats().length, 0);
			assert.strictEqual(consumer.getBackpressure(), 0);
		});

		it('should be able to calculate correct backpressure after invoking consumer.return()', async () => {
			const consumer = stream.createConsumer();

			for (let i = 0; i < 10; i++) {
				stream.write('hello' + i);
			}

			stream.close('end');

			await wait(10);
			consumer.return();

			assert.strictEqual(stream.getConsumerStats().length, 0);
			assert.strictEqual(consumer.getBackpressure(), 0);

			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hi' + i);
				}
				stream.close('end');
			})();

			const receivedPackets: IteratorResult<string>[] = [];

			let expectedPressure = 10;
			while (true) {
				const packet = await consumer.next();
				assert.strictEqual(consumer.getBackpressure(), expectedPressure);
				const consumerStatsList = stream.getConsumerStats();
				if (expectedPressure > 0) {
					assert.strictEqual(consumerStatsList.length, 1);
					assert.strictEqual(consumerStatsList[0]!.backpressure, expectedPressure);
				} else {
					assert.strictEqual(consumerStatsList.length, 0);
				}
				expectedPressure--;
				receivedPackets.push(packet);
				if (packet.done) break;
			}

			assert.strictEqual(receivedPackets.length, 11);
			assert.strictEqual(receivedPackets[0]!.value, 'hi0');
			assert.strictEqual(receivedPackets[9]!.value, 'hi9');
			assert.strictEqual(receivedPackets[10]!.done, true);
			assert.strictEqual(receivedPackets[10]!.value, 'end');

			assert.strictEqual(stream.getConsumerStats().length, 0);
			assert.strictEqual(consumer.getBackpressure(), 0);
		});
	});

	describe('await once', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should receive next packet asynchronously when once() method is used', async () => {
			(async () => {
				for (let i = 0; i < 3; i++) {
					await wait(10);
					stream.write('a' + i);
				}
			})();

			let nextPacket = await stream.once();
			assert.strictEqual(nextPacket, 'a0');

			nextPacket = await stream.once();
			assert.strictEqual(nextPacket, 'a1');

			nextPacket = await stream.once();
			assert.strictEqual(nextPacket, 'a2');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should throw an error if a number is passed to the once() method and it times out', async () => {
			(async () => {
				for (let i = 0; i < 3; i++) {
					await wait(20);
					stream.write('a' + i);
				}
			})();

			let nextPacket: string | undefined = await stream.once(30);
			assert.strictEqual(nextPacket, 'a0');

			let error: Error | undefined;
			nextPacket = undefined;
			try {
				nextPacket = await stream.once(10);
			} catch (err) {
				error = err as Error;
			}

			assert.strictEqual(nextPacket, undefined);
			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should not resolve once() call when stream.close() is called', async () => {
			(async () => {
				await wait(10);
				stream.close();
			})();

			const receivedPackets: (string | undefined)[] = [];

			(async () => {
				const nextPacket = await stream.once();
				receivedPackets.push(nextPacket);
			})();

			await wait(100);
			assert.strictEqual(receivedPackets.length, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should not resolve previous once() call after stream.close() is called', async () => {
			(async () => {
				await wait(10);
				stream.close();
				await wait(10);
				stream.write('foo');
			})();

			const receivedPackets: (string | undefined)[] = [];

			(async () => {
				const nextPacket = await stream.once();
				receivedPackets.push(nextPacket);
			})();

			await wait(100);
			assert.strictEqual(receivedPackets.length, 0);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should resolve once() if it is called after stream.close() is called and then a new packet is written', async () => {
			(async () => {
				await wait(10);
				stream.close();
				await wait(10);
				stream.write('foo');
			})();

			const receivedPackets: (string | undefined)[] = [];

			(async () => {
				const nextPacket = await stream.once();
				receivedPackets.push(nextPacket);
			})();

			await wait(100);

			assert.strictEqual(receivedPackets.length, 0);

			(async () => {
				await wait(10);
				stream.write('bar');
			})();

			const packet = await stream.once();
			assert.strictEqual(packet, 'bar');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});
	});

	describe('while loop with await inside', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should receive packets asynchronously', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const asyncIterator = stream.createConsumer();
			while (true) {
				const packet = await asyncIterator.next();
				if (packet.done) break;
				receivedPackets.push(packet.value);
			}
			assert.strictEqual(receivedPackets.length, 10);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should receive packets asynchronously if multiple packets are written sequentially', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(10);
					stream.write('a' + i);
					stream.write('b' + i);
					stream.write('c' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const asyncIterator = stream.createConsumer();
			while (true) {
				const packet = await asyncIterator.next();
				if (packet.done) break;
				receivedPackets.push(packet.value);
			}
			assert.strictEqual(receivedPackets.length, 30);
			assert.strictEqual(receivedPackets[0], 'a0');
			assert.strictEqual(receivedPackets[1], 'b0');
			assert.strictEqual(receivedPackets[2], 'c0');
			assert.strictEqual(receivedPackets[3], 'a1');
			assert.strictEqual(receivedPackets[4], 'b1');
			assert.strictEqual(receivedPackets[5], 'c1');
			assert.strictEqual(receivedPackets[29], 'c9');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to timeout the consumer if the stream is idle for too long', async () => {
			(async () => {
				for (let i = 0; i < 10; i++) {
					await wait(30);
					stream.write('hello' + i);
				}
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const asyncIterator = stream.createConsumer(20);
			let error: Error | undefined;
			try {
				while (true) {
					const packet = await asyncIterator.next();
					if (packet.done) break;
					receivedPackets.push(packet.value);
				}
			} catch (err) {
				error = err as Error;
			}
			assert.strictEqual(receivedPackets.length, 0);
			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to continue iterating if a single iteration times out', async () => {
			(async () => {
				await wait(20);
				stream.write('hello0');
				await wait(20);
				stream.write('hello1');
				await wait(40);
				stream.write('hello2');
				await wait(20);
				stream.write('hello3');
				await wait(20);
				stream.write('hello4');
				stream.close();
			})();

			const receivedPackets: string[] = [];
			const asyncIterator = stream.createConsumer(30);
			const errors: Error[] = [];

			while (true) {
				let packet;
				try {
					packet = await asyncIterator.next();
				} catch (err) {
					errors.push(err as Error);
					continue;
				}
				if (packet.done) break;
				receivedPackets.push(packet.value);
			}
			assert.strictEqual(receivedPackets.length, 5);
			assert.strictEqual(errors.length, 1);
			assert.notEqual(errors[0], null);
			assert.strictEqual(errors[0]!.name, 'TimeoutError');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});
	});

	describe('actions on an individual consumer', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should stop a specific consumer immediately when that consumer is killed', async () => {
			let backpressureBeforeKill;
			let backpressureAfterKill;

			const consumerA = stream.createConsumer();
			const consumerB = stream.createConsumer();

			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				backpressureBeforeKill = stream.getBackpressure();
				stream.killConsumer(consumerA.id, 'custom kill data');
				backpressureAfterKill = stream.getBackpressure();
				stream.close();
			})();

			const receivedPacketsA: IteratorResult<string>[] = [];
			const receivedPacketsB: IteratorResult<string>[] = [];

			await Promise.all([
				(async () => {
					while (true) {
						const packet = await consumerA.next();
						await wait(50);
						receivedPacketsA.push(packet);
						if (packet.done) break;
					}
				})(),
				(async () => {
					while (true) {
						const packet = await consumerB.next();
						await wait(50);
						receivedPacketsB.push(packet);
						if (packet.done) break;
					}
				})()
			]);

			const backpressureAfterConsume = stream.getBackpressure();

			assert.strictEqual(backpressureBeforeKill, 10);
			assert.strictEqual(backpressureAfterKill, 10); // consumerB was still running.
			assert.strictEqual(backpressureAfterConsume, 0);
			assert.strictEqual(receivedPacketsA.length, 1);
			assert.strictEqual(receivedPacketsA[0]!.done, true);
			assert.strictEqual(receivedPacketsA[0]!.value, 'custom kill data');
			assert.strictEqual(receivedPacketsB.length, 11);
			assert.strictEqual(receivedPacketsB[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsB[9]!.value, 'hello9');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should stop a specific consumer when that consumer is closed', async () => {
			let maxBackpressureBeforeClose;
			let maxBackpressureAfterClose;
			let backpressureBeforeCloseA;
			let backpressureBeforeCloseB;

			const consumerA = stream.createConsumer();
			const consumerB = stream.createConsumer();

			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				maxBackpressureBeforeClose = stream.getBackpressure();
				stream.closeConsumer(consumerA.id, 'custom close data');
				maxBackpressureAfterClose = stream.getBackpressure();
				stream.write('foo');
				backpressureBeforeCloseA = stream.getBackpressure(consumerA.id);
				backpressureBeforeCloseB = stream.getBackpressure(consumerB.id);
				stream.close('close others');
			})();

			const receivedPacketsA: IteratorResult<string>[] = [];
			const receivedPacketsB: IteratorResult<string>[] = [];

			await Promise.all([
				(async () => {
					while (true) {
						const packet = await consumerA.next();
						await wait(50);
						receivedPacketsA.push(packet);
						if (packet.done) break;
					}
				})(),
				(async () => {
					while (true) {
						const packet = await consumerB.next();
						await wait(50);
						receivedPacketsB.push(packet);
						if (packet.done) break;
					}
				})()
			]);

			const maxBackpressureAfterConsume = stream.getBackpressure();

			assert.strictEqual(backpressureBeforeCloseA, 12);
			assert.strictEqual(backpressureBeforeCloseB, 12);
			assert.strictEqual(maxBackpressureBeforeClose, 10);
			assert.strictEqual(maxBackpressureAfterClose, 11);
			assert.strictEqual(maxBackpressureAfterConsume, 0);
			assert.strictEqual(receivedPacketsA.length, 11);
			assert.strictEqual(receivedPacketsA[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsA[9]!.value, 'hello9');
			assert.strictEqual(receivedPacketsA[10]!.done, true);
			assert.strictEqual(receivedPacketsA[10]!.value, 'custom close data');
			assert.strictEqual(receivedPacketsB.length, 12);
			assert.strictEqual(receivedPacketsB[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsB[9]!.value, 'hello9');
			assert.strictEqual(receivedPacketsB[10]!.value, 'foo');
			assert.strictEqual(receivedPacketsB[11]!.done, true);
			assert.strictEqual(receivedPacketsB[11]!.value, 'close others');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should support closing only one of multiple consumers', async () => {
			let backpressureBeforeClose: number;
			let backpressureAfterClose: number;

			const consumerA = stream.createConsumer();
			const consumerB = stream.createConsumer();

			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				backpressureBeforeClose = stream.getBackpressure();
				stream.closeConsumer(consumerA.id, 'custom close data');
				backpressureAfterClose = stream.getBackpressure();
			})();

			const receivedPacketsA: IteratorResult<string>[] = [];
			const receivedPacketsB: IteratorResult<string>[] = [];

			await Promise.race([
				(async () => {
					while (true) {
						const packet = await consumerA.next();
						await wait(50);
						receivedPacketsA.push(packet);
						if (packet.done) break;
					}
				})(),
				(async () => {
					while (true) {
						const packet = await consumerB.next();
						await wait(50);
						receivedPacketsB.push(packet);
						if (packet.done) break;
					}
				})()
			]);

			const backpressureAfterConsume = stream.getBackpressure();

			assert.strictEqual(backpressureBeforeClose!, 10);
			assert.strictEqual(backpressureAfterClose!, 11);
			assert.strictEqual(backpressureAfterConsume, 0);
			assert.strictEqual(receivedPacketsA.length, 11);
			assert.strictEqual(receivedPacketsA[10]!.done, true);
			assert.strictEqual(receivedPacketsA[10]!.value, 'custom close data');
			assert.strictEqual(receivedPacketsB.length, 10);
			assert.strictEqual(receivedPacketsB[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsB[9]!.value, 'hello9');

			stream.close(consumerB.id.toString());
			await wait(10);

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});

		it('should be able to write to a specific consumer', async () => {
			const consumerA = stream.createConsumer();
			const consumerB = stream.createConsumer();

			(async () => {
				await wait(10);
				for (let i = 0; i < 10; i++) {
					stream.write('hello' + i);
				}
				for (let i = 0; i < 3; i++) {
					stream.writeToConsumer(consumerA.id, 'hi' + i);
				}
				stream.close('close all');
			})();

			const receivedPacketsA: IteratorResult<string>[] = [];
			const receivedPacketsB: IteratorResult<string>[] = [];

			await Promise.all([
				(async () => {
					while (true) {
						const packet = await consumerA.next();
						await wait(50);
						receivedPacketsA.push(packet);
						if (packet.done) break;
					}
				})(),
				(async () => {
					while (true) {
						const packet = await consumerB.next();
						await wait(50);
						receivedPacketsB.push(packet);
						if (packet.done) break;
					}
				})()
			]);

			const backpressureAfterConsume = stream.getBackpressure();

			assert.strictEqual(backpressureAfterConsume, 0);
			assert.strictEqual(receivedPacketsA.length, 14);
			assert.strictEqual(receivedPacketsA[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsA[9]!.value, 'hello9');
			assert.strictEqual(receivedPacketsA[10]!.value, 'hi0');
			assert.strictEqual(receivedPacketsA[12]!.value, 'hi2');
			assert.strictEqual(receivedPacketsA[13]!.done, true);
			assert.strictEqual(receivedPacketsA[13]!.value, 'close all');
			assert.strictEqual(receivedPacketsB.length, 11);
			assert.strictEqual(receivedPacketsB[0]!.value, 'hello0');
			assert.strictEqual(receivedPacketsB[9]!.value, 'hello9');
			assert.strictEqual(receivedPacketsB[10]!.done, true);
			assert.strictEqual(receivedPacketsB[10]!.value, 'close all');

			assert.strictEqual(stream.getConsumerCount(), 0); // Check internal cleanup.
		});
	});

	describe('consumer count', () => {
		beforeEach(async () => {
			stream = new WritableConsumableStream();
		});

		afterEach(async () => {
			cancelAllPendingWaits();
			stream.close();
		});

		it('should return the number of consumers as 1 after stream.createConsumer() is used once', async () => {
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			assert.strictEqual(stream.getConsumerCount(), 1);
		});

		it('should return the number of consumers as 2 after stream.createConsumer() is used twice', async () => {
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			assert.strictEqual(stream.getConsumerCount(), 2);
		});

		it('should return the number of consumers as 1 after stream is consumed directly by for-await-of loop once', async () => {
			(async () => {
				for await (const _ of stream) {}
			})();

			assert.strictEqual(stream.getConsumerCount(), 1);
		});

		it('should return the number of consumers as 2 after stream is consumed directly by for-await-of loop twice', async () => {
			(async () => {
				for await (const _ of stream) {}
			})();

			(async () => {
				for await (const _ of stream) {}
			})();

			assert.strictEqual(stream.getConsumerCount(), 2);
		});

		it('should return the number of consumers as 0 after stream is killed', async () => {
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			stream.kill();

			assert.strictEqual(stream.getConsumerCount(), 0);
		});

		it('should return the number of consumers as 1 after a specific consumer is killed', async () => {
			let consumerA: WritableStreamConsumer<string, string | undefined>;

			(async () => {
				consumerA = stream.createConsumer();
				for await (const _ of consumerA) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			stream.killConsumer(consumerA!.id);

			assert.strictEqual(stream.getConsumerCount(), 1);
		});

		it('should return the number of consumers as 0 after stream is close after last message has been consumed', async () => {
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			stream.close();

			assert.strictEqual(stream.getConsumerCount(), 3);

			await wait(1000);

			assert.strictEqual(stream.getConsumerCount(), 0);
		});

		it('should return the number of consumers as 1 after a specific consumer is closed after last message has been consumed', async () => {
			let consumerA: WritableStreamConsumer<string, string | undefined>;

			(async () => {
				consumerA = stream.createConsumer();
				for await (const _ of consumerA) {}
			})();
			(async () => {
				for await (const _ of stream.createConsumer()) {}
			})();

			stream.closeConsumer(consumerA!.id);

			assert.strictEqual(stream.getConsumerCount(), 2);

			await wait(1000);

			assert.strictEqual(stream.getConsumerCount(), 1);
		});
	});
});
