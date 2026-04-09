import { AsyncStreamEmitter } from '@socket-mesh/async-stream-emitter';
import { AuthEngine, defaultAuthEngine, isAuthEngine } from '@socket-mesh/auth-engine';
import { ChannelMap } from '@socket-mesh/channels';
import { ClientPrivateMap, ClientSocket, removeAuthTokenHandler, ServerPrivateMap } from '@socket-mesh/client';
import { AnyPacket, CallIdGenerator, HandlerMap, PrivateMethodMap, PublicMethodMap, ServiceMap, StreamCleanupMode, toError } from '@socket-mesh/core';
import { ServerProtocolError } from '@socket-mesh/errors';
import defaultCodec, { CodecEngine } from '@socket-mesh/formatter';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';
import { Server as HttpServer, IncomingMessage, OutgoingHttpHeaders } from 'http';
import { WebSocket, WebSocketServer } from 'ws';

import { Broker } from './broker/broker.js';
import { Exchange } from './broker/exchange.js';
import { SimpleBroker } from './broker/simple-broker.js';
import { CloseEvent, ConnectionEvent, ErrorEvent, HandshakeEvent, HeadersEvent, ListeningEvent, ServerEvent, SocketAuthenticateEvent, SocketAuthStateChangeEvent, SocketBadAuthTokenEvent, SocketCloseEvent, SocketConnectEvent, SocketConnectingEvent, SocketDeauthenticateEvent, SocketDisconnectEvent, SocketErrorEvent, SocketMessageEvent, SocketPingEvent, SocketPongEvent, SocketRemoveAuthTokenEvent, SocketRequestEvent, SocketResponseEvent, SocketSubscribeEvent, SocketSubscribeFailEvent, SocketSubscribeStateChangeEvent, SocketUnsubscribeEvent, WarningEvent } from './events/index.js';
import { authenticateHandler } from './handlers/authenticate.js';
import { handshakeHandler } from './handlers/handshake.js';
import { publishHandler } from './handlers/publish.js';
import { subscribeHandler } from './handlers/subscribe.js';
import { unsubscribeHandler } from './handlers/unsubscribe.js';
import { ServerPlugin } from './plugin/server-plugin.js';
import { ServerOptions } from './server-options.js';
import { ServerSocketState } from './server-socket-state.js';
import { ServerSocket } from './server-socket.js';

export class Server<
	TIncoming extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TOutgoing extends PublicMethodMap = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {},
	TState extends object = {}
> extends AsyncStreamEmitter<ServerEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>> {
	private readonly _callIdGenerator: CallIdGenerator;
	private _handlers:
	HandlerMap<
			TIncoming & TPrivateIncoming & ServerPrivateMap,
			TOutgoing,
			TPrivateOutgoing & ClientPrivateMap,
			TService,
			TState & ServerSocketState
	>;

	private _isListening: boolean;
	private _isReady: boolean;
	private _pingIntervalRef: NodeJS.Timeout | null;
	private readonly _wss: WebSocketServer;

	// | ServerSocket<TIncomingMap, TServiceMap, TOutgoingMap, TPrivateIncomingMap, TPrivateOutgoingMap, TServerState, TSocketState>
	public ackTimeoutMs: number;
	public allowClientPublish: boolean;
	public readonly auth: AuthEngine;
	public readonly brokerEngine!: Broker<TChannel>;
	public clientCount: number;
	public readonly clients: { [ id: string ]: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState> };
	public readonly codecEngine: CodecEngine;
	public readonly httpServer: HttpServer;

	public isPingTimeoutDisabled: boolean;
	public origins: string;
	public pendingClientCount: number;
	public readonly pendingClients: { [ id: string ]: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState> };
	public pingIntervalMs: number;
	public pingTimeoutMs: number;
	public readonly plugins: ServerPlugin<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>[];
	public socketChannelLimit?: number;
	public readonly socketStreamCleanupMode: StreamCleanupMode;

	public strictHandshake: boolean;

	constructor(options?: ServerOptions<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) {
		super();

		let cid = 1;

		if (!options) {
			options = {};
		}

		options.clientTracking = true;

		this._isListening = false;
		this._isReady = false;
		this._pingIntervalRef = null;
		this.ackTimeoutMs = options.ackTimeoutMs || 10000;
		this.allowClientPublish = options.allowClientPublish ?? true;
		this.auth = isAuthEngine(options.authEngine) ? options.authEngine : defaultAuthEngine(options.authEngine);
		this.brokerEngine = options.brokerEngine || new SimpleBroker<TChannel>();
		this._callIdGenerator = options.callIdGenerator || (() => {
			return cid++;
		});

		this.clients = {};
		this.clientCount = 0;
		this.codecEngine = options.codecEngine || defaultCodec;

		this._handlers = Object.assign(
			{
				'#authenticate': authenticateHandler,
				'#handshake': handshakeHandler,
				'#publish': publishHandler,
				'#removeAuthToken': removeAuthTokenHandler,
				'#subscribe': subscribeHandler,
				'#unsubscribe': unsubscribeHandler
			},
			options.handlers
		);
		this.httpServer = options.server!;

		this.plugins = options.plugins || [];
		this.origins = options.origins || '*:*';
		this.pendingClients = {};
		this.pendingClientCount = 0;
		this.isPingTimeoutDisabled = (options.isPingTimeoutDisabled === true);
		this.pingIntervalMs = options.pingIntervalMs || 8000;
		this.pingTimeoutMs = options.pingTimeoutMs || 20000;

		this.socketChannelLimit = options.socketChannelLimit;
		this.socketStreamCleanupMode = options.socketStreamCleanupMode || 'kill';
		this.strictHandshake = options.strictHandshake ?? true;

		options.verifyClient = this.verifyClient.bind(this);

		this._wss = new WebSocketServer(options);

		this._wss.on('close', this.onClose.bind(this));
		this._wss.on('connection', this.onConnection.bind(this));
		this._wss.on('error', this.onError.bind(this));
		this._wss.on('headers', this.onHeaders.bind(this));
		this._wss.on('listening', this.onListening.bind(this));

		(async () => {
			for await (const { error } of this.brokerEngine.listen('error')) {
				this.emit('warning', { warning: error });
			}
		})();

		if (this.brokerEngine.isReady) {
			setTimeout(() => {
				this._isReady = true;
				this.emit('ready', {});
			}, 0);
		} else {
			this._isReady = false;
			(async () => {
				await this.brokerEngine.listen('ready').once();
				this._isReady = true;
				this.emit('ready', {});
			})();
		}
	}

	public addPlugin(...plugin: ServerPlugin<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>[]): void {
		this.plugins.push(...plugin);
	}

	private bind(socket: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) {
		/*
		if (socket.type === 'client') {
			(async () => {
				for await (let event of socket.listen()) {
					this.emit(
						`socket${event.stream[0].toUpperCase()}${event.stream.substring(1)}` as any,
						Object.assign(
							{ socket },
							event.value
						)
					);
				}
			})();

			(async () => {
				for await (let event of socket.channels.listen()) {
					this.emit(
						`socket${event.stream[0].toUpperCase()}${event.stream.substring(1)}` as any,
						Object.assign(
							{ socket },
							event.value
						)
					);
				}
			})();
		}
*/

		(async () => {
			for await (const _ of socket.listen('connect')) {
				if (this.pendingClients[socket.id]) {
					delete this.pendingClients[socket.id];
					this.pendingClientCount--;
				}

				this.clients[socket.id] = socket;
				this.clientCount++;
				this.startPinging();
			}
		})();

		(async () => {
			for await (const _ of socket.listen('connectAbort')) {
				this.socketDisconnected(socket);
			}
		})();

		(async () => {
			for await (const _ of socket.listen('disconnect')) {
				this.socketDisconnected(socket);
			}
		})();
	}

	close(keepSocketsOpen?: boolean): Promise<void> {
		this._isListening = false;

		return new Promise<void>((resolve, reject) => {
			this._wss.close((err) => {
				if (err) {
					reject(err);
					return;
				}
				resolve();
			});

			if (!keepSocketsOpen) {
				for (const socket of Object.values(this.clients)) {
					socket.disconnect();
				}
			}
		});
	}

	emit(event: 'close', data: CloseEvent): void;
	emit(event: 'connection', data: ConnectionEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'error', data: ErrorEvent): void;
	emit(event: 'headers', data: HeadersEvent): void;
	emit(event: 'handshake', data: HandshakeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'listening', data: ListeningEvent): void;
	emit(event: 'ready', data: {}): void;
	emit(event: 'socketAuthStateChange', data: SocketAuthStateChangeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketAuthenticate', data: SocketAuthenticateEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketBadAuthToken', data: SocketBadAuthTokenEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketClose', data: SocketCloseEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketConnect', data: SocketConnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketConnectAbort', data: SocketDisconnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketConnecting', data: SocketConnectingEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketDeauthenticate', data: SocketDeauthenticateEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketDisconnect', data: SocketDisconnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketError', data: SocketErrorEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketMessage', data: SocketMessageEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketPing', data: SocketPingEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketPong', data: SocketPongEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketRemoveAuthToken', data: SocketRemoveAuthTokenEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketRequest', data: SocketRequestEvent<TService, TIncoming, TPrivateIncoming>): void;
	emit(event: 'socketResponse', data: SocketResponseEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketSubscribe', data: SocketSubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketSubscribeFail', data: SocketSubscribeFailEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketSubscribeRequest', data: SocketSubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketSubscribeStateChange', data: SocketSubscribeStateChangeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'socketUnsubscribe', data: SocketUnsubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>): void;
	emit(event: 'warning', data: WarningEvent): void;
	emit(event: string, data: any): void {
		super.emit(event, data);
	}

	public get exchange(): Exchange<TChannel> {
		return this.brokerEngine.exchange;
	}

	public get isListening(): boolean {
		return this._isListening;
	}

	public get isReady(): boolean {
		return this._isReady;
	}

	listen(): DemuxedConsumableStream<StreamEvent<ServerEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>>;
	listen(event: 'close'): DemuxedConsumableStream<CloseEvent>;
	listen(event: 'connection'): DemuxedConsumableStream<ConnectionEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'error'): DemuxedConsumableStream<ErrorEvent>;
	listen(event: 'handshake'): DemuxedConsumableStream<HandshakeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'headers'): DemuxedConsumableStream<HeadersEvent>;
	listen(event: 'listening'): DemuxedConsumableStream<ListeningEvent>;
	listen(event: 'ready'): DemuxedConsumableStream<{}>;
	listen(event: 'socketAuthStateChange'): DemuxedConsumableStream<SocketAuthStateChangeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketAuthenticate'): DemuxedConsumableStream<SocketAuthenticateEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketBadAuthToken'): DemuxedConsumableStream<SocketBadAuthTokenEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketClose'): DemuxedConsumableStream<SocketCloseEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketConnect'): DemuxedConsumableStream<SocketConnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketConnectAbort'): DemuxedConsumableStream<SocketDisconnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketConnecting'): DemuxedConsumableStream<SocketConnectingEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketDeauthenticate'): DemuxedConsumableStream<SocketDeauthenticateEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketDisconnect'): DemuxedConsumableStream<SocketDisconnectEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketError'): DemuxedConsumableStream<SocketErrorEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketMessage'): DemuxedConsumableStream<SocketMessageEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketPing'): DemuxedConsumableStream<SocketPingEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketPong'): DemuxedConsumableStream<SocketPongEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketRemoveAuthToken'): DemuxedConsumableStream<SocketRemoveAuthTokenEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketRequest'): DemuxedConsumableStream<SocketRequestEvent<TService, TIncoming, TPrivateIncoming>>;
	listen(event: 'socketResponse'): DemuxedConsumableStream<SocketResponseEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketSubscribe'): DemuxedConsumableStream<SocketSubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketSubscribeFail'): DemuxedConsumableStream<SocketSubscribeFailEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketSubscribeRequest'): DemuxedConsumableStream<SocketSubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketSubscribeStateChange'): DemuxedConsumableStream<SocketSubscribeStateChangeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'socketUnsubscribe'): DemuxedConsumableStream<SocketUnsubscribeEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>>;
	listen(event: 'warning'): DemuxedConsumableStream<WarningEvent>;
	listen(event?: string): DemuxedConsumableStream<ServerEvent<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>> | DemuxedConsumableStream<StreamEvent<any>> {
		return event ? super.listen(event) : super.listen();
	}

	// eslint-disable-next-line @typescript-eslint/no-unused-vars
	private onClose(code: string, reason: Buffer): void {
		this.emit('close', {});
	}

	private onConnection(wsSocket: WebSocket, upgradeReq: IncomingMessage): void {
		/*
		if (!wsSocket.upgradeReq) {
			// Normalize ws modules to match.
			wsSocket.upgradeReq = upgradeReq;
		}
*/
		const socket = new ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>({
			ackTimeoutMs: this.ackTimeoutMs,
			callIdGenerator: this._callIdGenerator,
			codecEngine: this.codecEngine,
			handlers: this._handlers,
			onUnhandledRequest: this.onUnhandledRequest.bind(this),
			plugins: this.plugins,
			request: upgradeReq,
			server: this,
			socket: wsSocket,
			state: {} as any,
			streamCleanupMode: this.socketStreamCleanupMode
		});

		this.pendingClientCount++;
		this.bind(this.pendingClients[socket.id] = socket);

		// ws.on('error', console.error);

		this.emit('connection', { socket, upgradeReq });

		// Emit event to signal that a socket handshake has been initiated.
		this.emit('handshake', { socket });
	}

	private onError(error: Error | string): void {
		if (typeof error === 'string') {
			error = new ServerProtocolError(error);
		}

		this.emit('error', { error });
	}

	private onHeaders(headers: string[], request: IncomingMessage): void {
		this.emit('headers', { headers, request });
	}

	private onListening(): void {
		this._isListening = true;

		this.emit('listening', {});
	}

	private onUnhandledRequest(
		socket:
			ClientSocket<PublicMethodMap, TChannel, TService, TState, TOutgoing & TPrivateOutgoing, TPrivateIncoming>
			| ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
		packet: AnyPacket<TIncoming & TPrivateIncoming & ServerPrivateMap, TService>
	): boolean {
		return false;
	}

	private socketDisconnected(
		socket:
		// ClientSocket<PublicMethodMap, TChannel, TService, TState, TOutgoing & TPrivateOutgoing, TPrivateIncoming> |
		ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
	): void {
		if (this.pendingClients[socket.id]) {
			delete this.pendingClients[socket.id];
			this.pendingClientCount--;
		}

		if (this.clients[socket.id]) {
			delete this.clients[socket.id];
			this.clientCount--;
		}

		if (this.clientCount <= 0) {
			this.stopPinging();
		}
	}

	private startPinging(): void {
		if (!this._pingIntervalRef && !this.isPingTimeoutDisabled) {
			this._pingIntervalRef = setInterval(() => {
				for (const id in this.clients) {
					this.clients[id]!
						.ping()
						.catch((err) => {
							this.onError(err);
						});
				}
			}, this.pingIntervalMs);
		}
	}

	private stopPinging(): void {
		if (this._pingIntervalRef) {
			clearInterval(this._pingIntervalRef);
			this._pingIntervalRef = null;
		}
	}

	private async verifyClient(
		info: { origin: string, req: IncomingMessage, secure: boolean },
		callback: (res: boolean, code?: number, message?: string, headers?: OutgoingHttpHeaders) => void
	): Promise<void> {
		try {
			if (typeof info.origin !== 'string' || info.origin === 'null') {
				info.origin = '*';
			}

			if (this.origins.indexOf('*:*') === -1) {
				let isOk = false;

				try {
					const url = new URL(info.origin);
					url.port = url.port || (url.protocol === 'https:' ? '443' : '80');
					isOk = !!(~this.origins.indexOf(url.hostname + ':' + url.port)
						|| ~this.origins.indexOf(url.hostname + ':*')
						|| ~this.origins.indexOf('*:' + url.port));
				} catch (e) {
					// Intentional
				}

				if (!isOk) {
					const error = new ServerProtocolError(
						`Failed to authorize socket handshake - Invalid origin: ${info.origin}`
					);

					this.emit('warning', { warning: error });

					callback(false, 403, error.message);
					return;
				}
			}

			try {
				for (const plugin of this.plugins) {
					if (plugin.onConnection) {
						await plugin.onConnection(info.req);
					}
				}
			} catch (err) {
				const error = toError(err);
				callback(false, 401, typeof err === 'string' ? err : error.message);
				return;
			}

			callback(true);
		} catch (err) {
			const error = toError(err);
			this.onError(error);
			this.emit('warning', { warning: error });

			callback(false, 403, typeof err === 'string' ? err : error.message);
		}
	}
}
