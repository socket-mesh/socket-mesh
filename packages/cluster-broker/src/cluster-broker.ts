import { ChannelMap, PublishOptions } from '@socket-mesh/channels';
import defaultCodec, { CodecEngine } from '@socket-mesh/formatter';
import { Broker, ExchangeClient, SimpleExchange } from '@socket-mesh/server/broker';
import { connect, Socket as NetSocket } from 'net';

import { ClusterBrokerOptions } from './cluster-broker-options.js';
import { encodeFrame, FrameDecoder } from './frame-codec.js';
import { WireMessage } from './wire.js';

/**
 * `Broker<T>` implementation that forwards publishes to a dedicated
 * broker process over a loopback TCP socket and delivers received
 * publishes to local exchange clients.
 *
 * Routing model
 * -------------
 * - Local subscribers (WebSocket clients attached to this worker) are
 *   tracked in-process in `_clientSubscribers`, exactly like
 *   `SimpleBroker`. They're delivered to synchronously on `publish`.
 * - Whenever this worker acquires its FIRST local subscriber on a
 *   channel, it sends a `subscribe` frame to the host so the host will
 *   start forwarding publishes on that channel to this worker.
 * - Whenever this worker drops its LAST local subscriber on a channel,
 *   it sends an `unsubscribe` frame.
 * - On local publish we deliver to local subscribers AND send a
 *   `publish` frame to the host; the host fans out to other workers
 *   subscribed to the channel and skips this origin connection, so
 *   local delivery is not duplicated.
 *
 * Connection lifecycle
 * --------------------
 * - The initial `connect` happens in the constructor. The base `Broker`
 *   marks `isReady = true` on the next tick regardless of the TCP link
 *   state (so `Server` can finish its own bring-up immediately); the
 *   actual TCP connect completes asynchronously in parallel.
 * - If the TCP connection drops, the broker reconnects with exponential
 *   backoff capped at 30s. On successful reconnect, all currently
 *   tracked subscriptions are re-sent to the new host connection so the
 *   worker resumes receiving messages without extra coordination.
 * - Publishes issued while the socket is down are dropped for
 *   cross-process delivery but still reach local subscribers. This
 *   matches SimpleBroker semantics for in-process traffic and mirrors
 *   how socket-cluster's SCBroker behaves during a host blip.
 */

const MAX_RECONNECT_DELAY_MS = 30_000;

export class ClusterBroker<T extends ChannelMap> extends Broker<T> {
	private readonly _clientSubscribers: { [channelName: string]: { [id: string]: ExchangeClient } };
	private readonly _clientSubscribersCounter: { [channelName: string]: number };
	private readonly _codec: CodecEngine;
	private _destroyed: boolean;
	private readonly _frameDecoder: FrameDecoder;
	private readonly _options: ClusterBrokerOptions;
	private _reconnectAttempt: number;
	private _reconnectTimer: NodeJS.Timeout | null;
	private _socket: NetSocket | null;
	private readonly _workerId: string;
	readonly exchange: SimpleExchange<T>;

	constructor(options: ClusterBrokerOptions) {
		super();

		this._clientSubscribers = {};
		this._clientSubscribersCounter = {};
		this._codec = options.codecEngine ?? defaultCodec;
		this._destroyed = false;
		this._frameDecoder = new FrameDecoder(this._codec);
		this._options = options;
		this._reconnectAttempt = 0;
		this._reconnectTimer = null;
		this._socket = null;
		this._workerId = options.workerId ?? `worker-${process.pid}`;

		this.exchange = new SimpleExchange(this);

		this.openConnection();
	}

	close(): Promise<void> {
		return new Promise((resolve) => {
			this._destroyed = true;

			if (this._reconnectTimer) {
				clearTimeout(this._reconnectTimer);
				this._reconnectTimer = null;
			}

			if (!this._socket) {
				resolve();
				return;
			}

			this._socket.once('close', () => {
				resolve();
			});
			this._socket.destroy();
		});
	}

	private async deliverLocal(channelName: string, data: unknown): Promise<void> {
		const packet: PublishOptions = { channel: channelName, data };
		const subscribers = this._clientSubscribers[channelName] || {};
		const work: Promise<void>[] = [];

		for (const id in subscribers) {
			work.push(subscribers[id]!.transmit('#publish', packet));
		}

		const result = await Promise.allSettled(work);

		for (const item of result) {
			if (item.status === 'rejected') {
				this.emit('error', { error: item.reason as Error });
			}
		}
	}

	invokePublish<U extends keyof T & string>(channelName: U, data: T[U], suppressEvent?: boolean): Promise<void> {
		return this.transmitPublish(channelName, data, suppressEvent);
	}

	isSubscribed(channelName: string): boolean {
		return !!this._clientSubscribers[channelName];
	}

	private onIncomingMessage(message: WireMessage): void {
		if (message.type !== 'publish') {
			return;
		}

		void this.deliverLocal(message.channel, message.data);
		this.emit('publish', { channel: message.channel, data: message.data as T[keyof T] });
	}

	private openConnection(): void {
		if (this._destroyed) {
			return;
		}

		const socket = connect({
			host: this._options.host ?? '127.0.0.1',
			port: this._options.port
		});

		socket.setNoDelay(true);

		this._socket = socket;

		socket.on('connect', () => {
			this._reconnectAttempt = 0;

			this.send({ type: 'hello', workerId: this._workerId });

			// Re-subscribe after a reconnect so the host forwards on the
			// channels we still care about locally.
			for (const channel of Object.keys(this._clientSubscribers)) {
				this.send({ channel, type: 'subscribe' });
			}
		});

		socket.on('data', (chunk: Buffer) => {
			try {
				const messages = this._frameDecoder.push(chunk);
				for (const message of messages) {
					this.onIncomingMessage(message);
				}
			} catch (err) {
				this.emit('error', { error: err as Error });
				socket.destroy();
			}
		});

		socket.on('error', (err) => {
			this.emit('error', { error: err });
		});

		socket.on('close', () => {
			this._socket = null;
			if (!this._destroyed) {
				this.scheduleReconnect();
			}
		});
	}

	private scheduleReconnect(): void {
		if (this._reconnectTimer) {
			return;
		}

		const base = this._options.reconnectBaseDelayMs ?? 200;
		const delay = Math.min(
			base * Math.pow(2, this._reconnectAttempt),
			MAX_RECONNECT_DELAY_MS
		);

		this._reconnectAttempt++;

		this._reconnectTimer = setTimeout(() => {
			this._reconnectTimer = null;
			this.openConnection();
		}, delay);
	}

	private send(message: WireMessage): void {
		if (!this._socket || this._socket.destroyed || !this._socket.writable) {
			return;
		}

		try {
			const frame = encodeFrame(this._codec, message);
			this._socket.write(frame);
		} catch (err) {
			this.emit('error', { error: err as Error });
		}
	}

	async subscribe(client: ExchangeClient, channelName: string): Promise<void> {
		let shouldForward = false;

		if (!this._clientSubscribers[channelName]) {
			this._clientSubscribers[channelName] = {};
			this._clientSubscribersCounter[channelName] = 0;
			this.emit('subscribe', { channel: channelName });
			shouldForward = true;
		}

		if (!this._clientSubscribers[channelName][client.id]) {
			this._clientSubscribersCounter[channelName]!++;
		}
		this._clientSubscribers[channelName][client.id] = client;

		if (shouldForward) {
			this.send({ channel: channelName, type: 'subscribe' });
		}
	}

	subscriptions(): string[] {
		return Object.keys(this._clientSubscribers);
	}

	async transmitPublish<U extends keyof T & string>(channelName: U, data: T[U], suppressEvent?: boolean): Promise<void> {
		await this.deliverLocal(channelName, data);
		this.send({ channel: channelName, data, type: 'publish' });

		if (!suppressEvent) {
			this.emit('publish', { channel: channelName, data });
		}
	}

	async unsubscribe(client: ExchangeClient, channelName: string): Promise<void> {
		if (!this._clientSubscribers[channelName]) {
			return;
		}
		if (!this._clientSubscribers[channelName][client.id]) {
			return;
		}

		this._clientSubscribersCounter[channelName]!--;
		delete this._clientSubscribers[channelName][client.id];

		if (this._clientSubscribersCounter[channelName]! <= 0) {
			delete this._clientSubscribers[channelName];
			delete this._clientSubscribersCounter[channelName];
			this.emit('unsubscribe', { channel: channelName });
			this.send({ channel: channelName, type: 'unsubscribe' });
		}
	}

	public get workerId(): string {
		return this._workerId;
	}
}
