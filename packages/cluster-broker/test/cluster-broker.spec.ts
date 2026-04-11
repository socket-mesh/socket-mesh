import { ChannelMap } from '@socket-mesh/channels';
import { ExchangeClient } from '@socket-mesh/server/broker';
import assert from 'node:assert';
import { AddressInfo, createServer } from 'node:net';
import { afterEach, beforeEach, describe, it } from 'node:test';

import { ClusterBroker, ClusterBrokerHost } from '../src/index.js';

interface Channels extends ChannelMap {
	bar: { value: number },
	foo: string
}

/**
 * Utility: a minimal `ExchangeClient` that records every `#publish`
 * transmit so tests can assert on the payloads delivered locally.
 */
function makeClient(id: string): ExchangeClient & { received: Array<{ channel: string, data: unknown }> } {
	const received: Array<{ channel: string, data: unknown }> = [];

	return {
		id,
		received,
		async transmit(_event, packet) {
			received.push({ channel: packet.channel, data: packet.data });
		}
	};
}

/**
 * Pick an ephemeral port from the OS by briefly listening on :0. The
 * returned port is released before we hand it back so the test can bind
 * the real broker host there.
 */
async function pickEphemeralPort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = createServer();
		server.unref();
		server.on('error', reject);
		server.listen(0, '127.0.0.1', () => {
			const address = server.address() as AddressInfo | null;
			const port = address?.port;
			server.close(() => {
				if (port === undefined) {
					reject(new Error('failed to pick ephemeral port'));
					return;
				}
				resolve(port);
			});
		});
	});
}

function wait(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

describe('ClusterBroker', () => {
	let host: ClusterBrokerHost;
	let port: number;

	beforeEach(async () => {
		port = await pickEphemeralPort();
		host = new ClusterBrokerHost({ port });
		await host.start();
	});

	afterEach(async () => {
		await host.close();
	});

	it('delivers local publishes synchronously without going through the host', async () => {
		const broker = new ClusterBroker<Channels>({ port });

		try {
			const subscriber = makeClient('local-sub');
			await broker.subscribe(subscriber, 'foo');

			await broker.transmitPublish('foo', 'hello');

			assert.deepStrictEqual(subscriber.received, [
				{ channel: 'foo', data: 'hello' }
			]);
		} finally {
			await broker.close();
		}
	});

	it('fans out publishes from one worker to another subscribed worker', async () => {
		const workerA = new ClusterBroker<Channels>({ port, workerId: 'worker-a' });
		const workerB = new ClusterBroker<Channels>({ port, workerId: 'worker-b' });

		try {
			// Wait for both TCP connections to come up. The host reports
			// workerConnect via listen('workerConnect'), but using a
			// short wait here keeps the test readable.
			await wait(75);

			const subscriberB = makeClient('b-sub');
			await workerB.subscribe(subscriberB, 'foo');

			// Give the subscribe frame time to reach the host and be
			// registered in the channel table.
			await wait(50);

			await workerA.transmitPublish('foo', 'crosses-the-wire');

			await wait(50);

			assert.deepStrictEqual(subscriberB.received, [
				{ channel: 'foo', data: 'crosses-the-wire' }
			]);
		} finally {
			await workerA.close();
			await workerB.close();
		}
	});

	it('does not echo a worker\'s publish back to its own subscribers via the host', async () => {
		// Both workers subscribe to the same channel. Worker A publishes;
		// subscriber A should see exactly one delivery (the local one),
		// NOT a second delivery bounced through the host.
		const workerA = new ClusterBroker<Channels>({ port, workerId: 'worker-a' });
		const workerB = new ClusterBroker<Channels>({ port, workerId: 'worker-b' });

		try {
			await wait(75);

			const subscriberA = makeClient('a-sub');
			const subscriberB = makeClient('b-sub');

			await workerA.subscribe(subscriberA, 'bar');
			await workerB.subscribe(subscriberB, 'bar');

			await wait(50);

			await workerA.transmitPublish('bar', { value: 42 });

			await wait(50);

			assert.strictEqual(subscriberA.received.length, 1, 'no self-echo');
			assert.deepStrictEqual(subscriberA.received[0], { channel: 'bar', data: { value: 42 } });

			assert.strictEqual(subscriberB.received.length, 1, 'remote delivery');
			assert.deepStrictEqual(subscriberB.received[0], { channel: 'bar', data: { value: 42 } });
		} finally {
			await workerA.close();
			await workerB.close();
		}
	});

	it('stops forwarding to a worker after it unsubscribes', async () => {
		const workerA = new ClusterBroker<Channels>({ port });
		const workerB = new ClusterBroker<Channels>({ port });

		try {
			await wait(75);

			const subscriberB = makeClient('b-sub');
			await workerB.subscribe(subscriberB, 'foo');
			await wait(50);

			await workerA.transmitPublish('foo', 'first');
			await wait(50);

			await workerB.unsubscribe(subscriberB, 'foo');
			await wait(50);

			await workerA.transmitPublish('foo', 'second');
			await wait(50);

			assert.deepStrictEqual(subscriberB.received, [
				{ channel: 'foo', data: 'first' }
			]);
		} finally {
			await workerA.close();
			await workerB.close();
		}
	});

	it('tracks connected workers on the host', async () => {
		const workerA = new ClusterBroker<Channels>({ port });
		const workerB = new ClusterBroker<Channels>({ port });

		try {
			await wait(75);
			assert.strictEqual(host.workerCount, 2);
		} finally {
			await workerA.close();
			await workerB.close();
		}

		await wait(50);
		assert.strictEqual(host.workerCount, 0);
	});
});
