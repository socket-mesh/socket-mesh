import { AsyncStreamEmitter } from '@socket-mesh/async-stream-emitter';
import defaultCodec, { CodecEngine } from '@socket-mesh/formatter';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';
import { AddressInfo, createServer, Server as NetServer, Socket as NetSocket } from 'net';

import { ClusterBrokerHostOptions } from './cluster-broker-options.js';
import { encodeFrame, FrameDecoder } from './frame-codec.js';
import { PublishMessage, WireMessage } from './wire.js';

export type ClusterBrokerHostEvent =
	| HostErrorEvent
	| HostListeningEvent
	| HostPublishEvent
	| HostWorkerConnectEvent
	| HostWorkerDisconnectEvent;

export interface HostErrorEvent {
	error: Error,
	workerId?: string
}

export interface HostListeningEvent {
	address: string,
	port: number
}

export interface HostPublishEvent {
	channel: string,
	data: unknown,
	fanoutCount: number,
	originWorkerId: string
}

export interface HostWorkerConnectEvent {
	workerId: string
}

export interface HostWorkerDisconnectEvent {
	workerId: string
}

interface WorkerConnection {
	buffer: FrameDecoder,
	socket: NetSocket,
	subscriptions: Set<string>,
	workerId: string
}

/**
 * Broker relay that runs in the cluster "master" (or any dedicated broker
 * process) and fans pub/sub traffic out across all connected worker
 * processes. Each worker uses a `ClusterBroker` instance to connect to the
 * host over a loopback TCP socket; publishes arrive on one connection and
 * are forwarded to every other connection subscribed to the channel.
 *
 * The origin connection is intentionally skipped during fanout: a worker
 * has already delivered its own publishes to local subscribers before
 * forwarding them here, so echoing would cause duplicate delivery.
 *
 * Worker identity is tracked from the `hello` frame for diagnostics only;
 * the host keys its subscription tables by connection object, so two
 * workers can share a worker id (e.g. after a restart) without colliding.
 */
export class ClusterBrokerHost extends AsyncStreamEmitter<ClusterBrokerHostEvent> {
	private readonly _channelSubscribers: Map<string, Set<WorkerConnection>>;
	private readonly _codec: CodecEngine;
	private readonly _options: ClusterBrokerHostOptions;
	private _server: NetServer | null;
	private readonly _workers: Set<WorkerConnection>;

	constructor(options: ClusterBrokerHostOptions) {
		super();

		this._channelSubscribers = new Map();
		this._codec = options.codecEngine ?? defaultCodec;
		this._options = options;
		this._server = null;
		this._workers = new Set();
	}

	private cleanupWorker(worker: WorkerConnection): void {
		if (!this._workers.delete(worker)) {
			return;
		}

		for (const channel of worker.subscriptions) {
			const set = this._channelSubscribers.get(channel);
			if (set) {
				set.delete(worker);
				if (set.size === 0) {
					this._channelSubscribers.delete(channel);
				}
			}
		}

		worker.subscriptions.clear();
		this.emit('workerDisconnect', { workerId: worker.workerId });
	}

	close(): Promise<void> {
		return new Promise((resolve, reject) => {
			if (!this._server) {
				resolve();
				return;
			}

			for (const worker of this._workers) {
				worker.socket.destroy();
			}

			this._server.close((err) => {
				this._server = null;
				if (err) {
					reject(err);
					return;
				}
				resolve();
			});
		});
	}

	emit(event: 'error', data: HostErrorEvent): void;
	emit(event: 'listening', data: HostListeningEvent): void;
	emit(event: 'publish', data: HostPublishEvent): void;
	emit(event: 'workerConnect', data: HostWorkerConnectEvent): void;
	emit(event: 'workerDisconnect', data: HostWorkerDisconnectEvent): void;
	emit(event: string, data: ClusterBrokerHostEvent): void {
		super.emit(event, data);
	}

	private fanout(origin: WorkerConnection, message: PublishMessage): void {
		const subscribers = this._channelSubscribers.get(message.channel);
		let fanoutCount = 0;

		if (subscribers) {
			const frame = encodeFrame(this._codec, message);

			for (const subscriber of subscribers) {
				if (subscriber === origin) {
					continue;
				}
				subscriber.socket.write(frame);
				fanoutCount++;
			}
		}

		this.emit('publish', {
			channel: message.channel,
			data: message.data,
			fanoutCount,
			originWorkerId: origin.workerId
		});
	}

	listen(): DemuxedConsumableStream<StreamEvent<ClusterBrokerHostEvent>>;
	listen(event: 'error'): DemuxedConsumableStream<HostErrorEvent>;
	listen(event: 'listening'): DemuxedConsumableStream<HostListeningEvent>;
	listen(event: 'publish'): DemuxedConsumableStream<HostPublishEvent>;
	listen(event: 'workerConnect'): DemuxedConsumableStream<HostWorkerConnectEvent>;
	listen(event: 'workerDisconnect'): DemuxedConsumableStream<HostWorkerDisconnectEvent>;
	listen<U extends ClusterBrokerHostEvent, V = U>(event: string): DemuxedConsumableStream<V>;
	listen<U extends ClusterBrokerHostEvent, V = U>(event?: string): DemuxedConsumableStream<V> {
		return super.listen(event ?? '');
	}

	private onConnection(socket: NetSocket): void {
		const worker: WorkerConnection = {
			buffer: new FrameDecoder(this._codec),
			socket,
			subscriptions: new Set(),
			workerId: `<unidentified:${socket.remoteAddress}:${socket.remotePort}>`
		};

		this._workers.add(worker);

		socket.setNoDelay(true);

		socket.on('data', (chunk: Buffer) => {
			try {
				const messages = worker.buffer.push(chunk);
				for (const message of messages) {
					this.onMessage(worker, message);
				}
			} catch (err) {
				this.emit('error', { error: err as Error, workerId: worker.workerId });
				worker.socket.destroy();
			}
		});

		socket.on('error', (err) => {
			this.emit('error', { error: err, workerId: worker.workerId });
		});

		socket.on('close', () => {
			this.cleanupWorker(worker);
		});
	}

	private onMessage(worker: WorkerConnection, message: WireMessage): void {
		switch (message.type) {
			case 'hello':
				worker.workerId = message.workerId;
				this.emit('workerConnect', { workerId: worker.workerId });
				break;

			case 'publish':
				this.fanout(worker, message);
				break;

			case 'subscribe': {
				if (worker.subscriptions.has(message.channel)) {
					break;
				}
				worker.subscriptions.add(message.channel);
				let set = this._channelSubscribers.get(message.channel);
				if (!set) {
					set = new Set();
					this._channelSubscribers.set(message.channel, set);
				}
				set.add(worker);
				break;
			}

			case 'unsubscribe':
				this.removeSubscription(worker, message.channel);
				break;
		}
	}

	private removeSubscription(worker: WorkerConnection, channel: string): void {
		if (!worker.subscriptions.delete(channel)) {
			return;
		}

		const set = this._channelSubscribers.get(channel);

		if (set) {
			set.delete(worker);
			if (set.size === 0) {
				this._channelSubscribers.delete(channel);
			}
		}
	}

	start(): Promise<void> {
		return new Promise((resolve, reject) => {
			if (this._server) {
				resolve();
				return;
			}

			const server = createServer((socket) => {
				this.onConnection(socket);
			});

			server.on('error', (err) => {
				this.emit('error', { error: err });
				if (!server.listening) {
					reject(err);
				}
			});

			server.listen(this._options.port, this._options.host ?? '127.0.0.1', () => {
				this._server = server;
				const info = server.address() as AddressInfo | null;
				this.emit('listening', {
					address: info?.address ?? '127.0.0.1',
					port: info?.port ?? this._options.port
				});
				resolve();
			});
		});
	}

	public get workerCount(): number {
		return this._workers.size;
	}
}
