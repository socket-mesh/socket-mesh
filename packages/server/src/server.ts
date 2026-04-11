import { AsyncStreamEmitter } from '@socket-mesh/async-stream-emitter';
import { AuthEngine, defaultAuthEngine, isAuthEngine } from '@socket-mesh/auth-engine';
import { ChannelMap } from '@socket-mesh/channels';
import { removeAuthTokenHandler } from '@socket-mesh/client';
import { CallIdGenerator, HandlerMap, LooseHandlerMap, MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap, StreamCleanupMode, toError } from '@socket-mesh/core';
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
import { ServerTransport } from './server-transport.js';

export class Server<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
> extends AsyncStreamEmitter<ServerEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>> {
	private readonly _callIdGenerator: CallIdGenerator;

	private _isListening: boolean;
	private _isReady: boolean;
	private _pingIntervalRef: NodeJS.Timeout | null;
	private readonly _serviceHandlers: { [service: string]: LooseHandlerMap };
	private readonly _wss: WebSocketServer;

	// | ServerSocket<TIncomingMap, TServiceMap, TOutgoingMap, TPrivateIncomingMap, TPrivateOutgoingMap, TServerState, TSocketState>
	public ackTimeoutMs: number;
	public allowClientPublish: boolean;
	public readonly auth: AuthEngine;
	public readonly brokerEngine!: Broker<TChannel>;
	public clientCount: number;
	public readonly clients: { [ id: string ]: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState> };
	public readonly codecEngine: CodecEngine;
	public readonly httpServer: HttpServer;

	public isPingTimeoutDisabled: boolean;
	public origins: string;
	public pendingClientCount: number;
	public readonly pendingClients: { [ id: string ]: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState> };
	public pingIntervalMs: number;
	public pingTimeoutMs: number;
	public readonly plugins: ServerPlugin<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>[];
	public socketChannelLimit?: number;
	public readonly socketStreamCleanupMode: StreamCleanupMode;

	public strictHandshake: boolean;

	constructor(options?: ServerOptions<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) {
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

		// Flat handlers live under the empty-string service key so dispatch
		// (and Server.addHandlers/removeHandlers) only has to consult one map.
		this._serviceHandlers = {
			'': Object.assign(
				{
					'#authenticate': authenticateHandler,
					'#handshake': handshakeHandler,
					'#publish': publishHandler,
					'#removeAuthToken': removeAuthTokenHandler,
					'#subscribe': subscribeHandler,
					'#unsubscribe': unsubscribeHandler
				},
				options.handlers
			)
		};

		if (options.serviceHandlers) {
			for (const service of Object.keys(options.serviceHandlers)) {
				this._serviceHandlers[service] = { ...options.serviceHandlers[service] };
			}
		}

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

	/**
	 * Register a group of strongly-typed request handlers under a service name.
	 *
	 * Handlers added this way can be added or replaced after the server has
	 * started and are immediately visible to all existing and future
	 * connections (the underlying handler map is shared by reference). The
	 * service name is surfaced via {@link services} so UI/tooling can list
	 * which groups are currently installed.
	 *
	 * When the service name is known to the server's `TService` generic,
	 * TypeScript validates the handler shape against the declared method map.
	 * For ad-hoc/dynamic services not present in `TService`, pass an explicit
	 * generic argument with the method map for the new service.
	 *
	 * @example
	 * // Statically declared on the server generic:
	 * server.addHandlers('account', { find: async (args) => { ... } });
	 *
	 * @example
	 * // Dynamically added from a module at runtime:
	 * server.addHandlers<'inventory', InventoryMethodMap>('inventory', handlers);
	 */
	public addHandlers<
		TServiceName extends keyof TService & string
	>(
		service: TServiceName,
		handlers: HandlerMap<
			TService[TServiceName],
			TState & ServerSocketState,
			ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
			ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
		>
	): void;
	public addHandlers<
		TServiceName extends string,
		TServiceMethodMap extends MethodMap
	>(
		service: TServiceName,
		handlers: HandlerMap<
			TServiceMethodMap,
			TState & ServerSocketState,
			ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
			ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
		>
	): void;
	public addHandlers(service: string, handlers: LooseHandlerMap): void {
		if (!this._serviceHandlers[service]) {
			this._serviceHandlers[service] = {};
		}

		Object.assign(this._serviceHandlers[service], handlers);
	}

	public addPlugin(...plugin: ServerPlugin<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>[]): void {
		this.plugins.push(...plugin);
	}

	private bind(socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) {
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
	emit(event: 'connection', data: ConnectionEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'error', data: ErrorEvent): void;
	emit(event: 'headers', data: HeadersEvent): void;
	emit(event: 'handshake', data: HandshakeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'listening', data: ListeningEvent): void;
	emit(event: 'ready', data: {}): void;
	emit(event: 'socketAuthStateChange', data: SocketAuthStateChangeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketAuthenticate', data: SocketAuthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketBadAuthToken', data: SocketBadAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketClose', data: SocketCloseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketConnect', data: SocketConnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketConnectAbort', data: SocketDisconnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketConnecting', data: SocketConnectingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketDeauthenticate', data: SocketDeauthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketDisconnect', data: SocketDisconnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketError', data: SocketErrorEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketMessage', data: SocketMessageEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketPing', data: SocketPingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketPong', data: SocketPongEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketRemoveAuthToken', data: SocketRemoveAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketRequest', data: SocketRequestEvent<TIncoming, TService, TPrivateIncoming>): void;
	emit(event: 'socketResponse', data: SocketResponseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketSubscribe', data: SocketSubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketSubscribeFail', data: SocketSubscribeFailEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketSubscribeRequest', data: SocketSubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketSubscribeStateChange', data: SocketSubscribeStateChangeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'socketUnsubscribe', data: SocketUnsubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>): void;
	emit(event: 'warning', data: WarningEvent): void;
	emit(event: string, data: any): void {
		super.emit(event, data);
	}

	public get exchange(): Exchange<TChannel> {
		return this.brokerEngine.exchange;
	}

	/** Method names registered under a given service, or an empty array if none. */
	public getServiceMethods(service: string): string[] {
		if (service === '') {
			// The empty-string slot stores flat (non-service) handlers internally
			// and is not part of the public service surface.
			return [];
		}

		const group = this._serviceHandlers[service];
		return group ? Object.keys(group) : [];
	}

	public get isListening(): boolean {
		return this._isListening;
	}

	public get isReady(): boolean {
		return this._isReady;
	}

	listen(): DemuxedConsumableStream<StreamEvent<ServerEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>>;
	listen(event: 'close'): DemuxedConsumableStream<CloseEvent>;
	listen(event: 'connection'): DemuxedConsumableStream<ConnectionEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'error'): DemuxedConsumableStream<ErrorEvent>;
	listen(event: 'handshake'): DemuxedConsumableStream<HandshakeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'headers'): DemuxedConsumableStream<HeadersEvent>;
	listen(event: 'listening'): DemuxedConsumableStream<ListeningEvent>;
	listen(event: 'ready'): DemuxedConsumableStream<{}>;
	listen(event: 'socketAuthStateChange'): DemuxedConsumableStream<SocketAuthStateChangeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketAuthenticate'): DemuxedConsumableStream<SocketAuthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketBadAuthToken'): DemuxedConsumableStream<SocketBadAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketClose'): DemuxedConsumableStream<SocketCloseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketConnect'): DemuxedConsumableStream<SocketConnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketConnectAbort'): DemuxedConsumableStream<SocketDisconnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketConnecting'): DemuxedConsumableStream<SocketConnectingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketDeauthenticate'): DemuxedConsumableStream<SocketDeauthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketDisconnect'): DemuxedConsumableStream<SocketDisconnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketError'): DemuxedConsumableStream<SocketErrorEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketMessage'): DemuxedConsumableStream<SocketMessageEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketPing'): DemuxedConsumableStream<SocketPingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketPong'): DemuxedConsumableStream<SocketPongEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketRemoveAuthToken'): DemuxedConsumableStream<SocketRemoveAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketRequest'): DemuxedConsumableStream<SocketRequestEvent<TIncoming, TService, TPrivateIncoming>>;
	listen(event: 'socketResponse'): DemuxedConsumableStream<SocketResponseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketSubscribe'): DemuxedConsumableStream<SocketSubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketSubscribeFail'): DemuxedConsumableStream<SocketSubscribeFailEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketSubscribeRequest'): DemuxedConsumableStream<SocketSubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketSubscribeStateChange'): DemuxedConsumableStream<SocketSubscribeStateChangeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'socketUnsubscribe'): DemuxedConsumableStream<SocketUnsubscribeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>>;
	listen(event: 'warning'): DemuxedConsumableStream<WarningEvent>;
	listen(event?: string): DemuxedConsumableStream<ServerEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>> | DemuxedConsumableStream<StreamEvent<any>> {
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
		const socket = new ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>({
			ackTimeoutMs: this.ackTimeoutMs,
			callIdGenerator: this._callIdGenerator,
			codecEngine: this.codecEngine,
			plugins: this.plugins,
			request: upgradeReq,
			server: this,
			serviceHandlers: this._serviceHandlers,
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

	/**
	 * Unregister either a whole service (when `methods` is omitted) or a
	 * specific set of methods within a service. Removing a service empties
	 * the group and drops the key so it no longer appears in {@link services}.
	 */
	public removeHandlers(service: string, methods?: readonly string[] | string): void {
		const group = this._serviceHandlers[service];

		if (!group) {
			return;
		}

		if (methods === undefined) {
			delete this._serviceHandlers[service];
			return;
		}

		const list = typeof methods === 'string' ? [methods] : methods;

		for (const method of list) {
			delete group[method];
		}

		if (Object.keys(group).length === 0) {
			delete this._serviceHandlers[service];
		}
	}

	/** Names of all service handler groups currently registered on the server. */
	public get services(): string[] {
		// The empty-string slot holds flat (non-service) handlers internally
		// and is not part of the public service surface.
		return Object.keys(this._serviceHandlers).filter(service => service !== '');
	}

	private socketDisconnected(
		socket:
		// ClientSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateOutgoing> |
		ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
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
