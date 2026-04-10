import { AuthToken, extractAuthTokenData, SignedAuthToken } from '@socket-mesh/auth';
import { AbortError, AuthError, BadConnectionError, dehydrateError, hydrateError, InvalidActionError, InvalidArgumentsError, PluginBlockedError, SocketClosedError, SocketProtocolError, SocketProtocolErrorStatuses, SocketProtocolIgnoreStatuses, TimeoutError } from '@socket-mesh/errors';
import defaultCodec, { CodecEngine } from '@socket-mesh/formatter';
import ws from 'isomorphic-ws';

import { HandlerMap } from './maps/handler-map.js';
import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap } from './maps/method-map.js';
import { AnyPacket, isRequestPacket } from './packet.js';
import { Plugin } from './plugins/plugin.js';
import { RequestHandlerArgs } from './request-handler.js';
import { abortRequest, AnyRequest, InvokeMethodRequest, InvokeServiceRequest, isRequestDone, TransmitMethodRequest, TransmitServiceRequest } from './request.js';
import { AnyResponse, DataResponse, isResponsePacket } from './response.js';
import { Socket, SocketOptions, SocketStatus, StreamCleanupMode } from './socket.js';
import { toArray, toError, wait } from './utils.js';

export type CallIdGenerator = () => number;

export interface InboundMessage {
	packet:
		(AnyPacket | AnyResponse)[]
		| (AnyPacket | AnyResponse) | null,
	timestamp: Date
}

interface InvokeCallback<T> {
	callback: (err: Error | null, result?: T) => void,
	method: string,
	timeoutId?: NodeJS.Timeout
}

export interface InvokeMethodOptions<
	TMethodMap extends MethodMap = MethodMap,
	TMethod extends keyof TMethodMap & string = keyof TMethodMap & string
> {
	ackTimeoutMs?: false | number,
	method: TMethod
}

export interface InvokeServiceOptions<
	TServiceMap extends ServiceMap = ServiceMap,
	TService extends keyof TServiceMap & string = keyof TServiceMap & string,
	TMethod extends keyof TServiceMap[TService] & string = keyof TServiceMap[TService] & string
> {
	ackTimeoutMs?: false | number,
	method: TMethod,
	service: TService
}

interface WebSocketError extends Error {
	code?: string
}

export class SocketTransport<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> {
	private _authToken: AuthToken | null;
	private readonly _callbackMap: { [cid: number]: InvokeCallback<unknown> };
	private readonly _callIdGenerator: CallIdGenerator;
	private readonly _handlers: HandlerMap<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>;
	private _inboundProcessedMessageCount: number;
	private _inboundReceivedMessageCount: number;
	private _isReady: boolean;
	private _outboundPreparedMessageCount: number;
	private _outboundSentMessageCount: number;
	private _pingTimeoutRef: NodeJS.Timeout | null;
	private _signedAuthToken: null | SignedAuthToken;
	private _socket!: Socket<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>;
	private _webSocket: null | ws.WebSocket;
	public ackTimeoutMs: number;
	public readonly codecEngine: CodecEngine;
	public id: null | string;
	public readonly plugins: Plugin<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>[];

	public streamCleanupMode: StreamCleanupMode;

	protected constructor(options?: SocketOptions<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>) {
		let cid = 1;

		this._isReady = false;
		this._authToken = null;
		this._signedAuthToken = null;
		this._webSocket = null;

		this.ackTimeoutMs = options?.ackTimeoutMs ?? 10000;

		this._callIdGenerator = options?.callIdGenerator || (() => {
			return cid++;
		});

		this._callbackMap = {};
		this.codecEngine = options?.codecEngine || defaultCodec;
		this._handlers = options?.handlers || {};
		this.id = null;
		this._inboundProcessedMessageCount = 0;
		this._inboundReceivedMessageCount = 0;
		this._outboundPreparedMessageCount = 0;
		this._outboundSentMessageCount = 0;
		this._pingTimeoutRef = null;
		this.plugins = options?.plugins || [];
		this.streamCleanupMode = options?.streamCleanupMode || 'kill';
	}

	protected abortAllPendingCallbacksDueToBadConnection(status: SocketStatus): void {
		for (const cid in this._callbackMap) {
			const map = this._callbackMap[cid]!;
			const msg = `Event ${map.method} was aborted due to a bad connection`;

			map.callback(
				new BadConnectionError(msg, status === 'ready' ? 'disconnect' : 'connectAbort')
			);
		}
	}

	public get authToken(): AuthToken | null {
		return this._authToken;
	}

	public async changeToUnauthenticatedState(): Promise<boolean> {
		if (this._signedAuthToken) {
			const authToken = this._authToken;
			const signedAuthToken = this._signedAuthToken;

			this._authToken = null;
			this._signedAuthToken = null;

			this._socket.emit('authStateChange', { isAuthenticated: false, wasAuthenticated: true });

			// In order for the events to trigger we need to wait for the next tick.
			await wait(0);

			this._socket.emit('deauthenticate', { authToken, signedAuthToken });

			for (const plugin of this.plugins) {
				if (plugin.onDeauthenticate) {
					plugin.onDeauthenticate({ socket: this.socket, transport: this });
				}
			}

			return true;
		}

		return false;
	}

	protected decode(data: string | ws.RawData): any {
		try {
			return this.codecEngine.decode(data as string);
		} catch (err) {
			const error = toError(err);

			if (error.name === 'Error') {
				error.name = 'InvalidMessageError';
			}

			this.onError(error);
			return null;
		}
	}

	public disconnect(code = 1000, reason?: string): void {
		if (this.webSocket) {
			this.webSocket.close(code, reason);
			this.onClose(code, reason);
		}
	}

	public getBackpressure(): number {
		return Math.max(
			this.getInboundBackpressure(),
			this.getOutboundBackpressure()
		);
	}

	public getInboundBackpressure(): number {
		return this._inboundReceivedMessageCount - this._inboundProcessedMessageCount;
	}

	public getOutboundBackpressure(): number {
		return this._outboundPreparedMessageCount - this._outboundSentMessageCount;
	}

	protected async handleInboudMessage({ packet, timestamp }: InboundMessage): Promise<void> {
		if (packet === null) {
			return;
		}

		packet = toArray<AnyPacket | AnyResponse>(packet);

		for (let curPacket of packet) {
			let pluginError: Error | undefined;

			try {
				for (const plugin of this.plugins) {
					if (plugin.onMessage) {
						curPacket = await plugin.onMessage({ packet: curPacket, socket: this.socket, timestamp: timestamp, transport: this });
					}
				}
			} catch (err) {
				pluginError = toError(err);
			}

			// Check to see if it is a request or response packet.
			if (isResponsePacket(curPacket)) {
				this.onResponse(curPacket, pluginError);
			} else if (isRequestPacket(curPacket)) {
				await this.onRequest(curPacket, timestamp, pluginError);
			} else {
				// TODO: Handle non packets here (binary data)
			}
		}
	}

	public invoke(
		methodOptions: [string, string, (false | number)?] | InvokeMethodOptions | InvokeServiceOptions | string,
		arg?: unknown
	): [Promise<unknown>, () => void] {
		let methodRequest: Omit<InvokeMethodRequest, 'promise'> | undefined;
		let serviceRequest: Omit<InvokeServiceRequest, 'promise'> | undefined;
		let service: string | undefined;
		let ackTimeoutMs: false | number | undefined;

		if (typeof methodOptions === 'object' && !Array.isArray(methodOptions)) {
			ackTimeoutMs = methodOptions.ackTimeoutMs;
		}

		if (typeof methodOptions === 'object' && (Array.isArray(methodOptions) || 'service' in methodOptions)) {
			let serviceMethod: string | undefined;

			if (Array.isArray(methodOptions)) {
				service = methodOptions[0];
				serviceMethod = methodOptions[1];
				ackTimeoutMs = methodOptions[2];
			} else {
				service = methodOptions.service;
				serviceMethod = methodOptions.method;
			}

			serviceRequest = {
				ackTimeoutMs: ackTimeoutMs || this.ackTimeoutMs,
				callback: null,
				cid: this._callIdGenerator(),
				data: arg,
				method: serviceMethod,
				service: service
			};
		} else {
			methodRequest = {
				ackTimeoutMs: ackTimeoutMs || this.ackTimeoutMs,
				callback: null,
				cid: this._callIdGenerator(),
				data: arg,
				method: (typeof methodOptions === 'object') ? methodOptions.method : methodOptions
			};
		}

		const callbackMap = this._callbackMap;

		let abort: () => void;
		const baseRequest = (serviceRequest || methodRequest!);

		const promise = new Promise<unknown>((resolve, reject) => {
			if (baseRequest.ackTimeoutMs) {
				baseRequest.timeoutId = setTimeout(
					() => {
						delete callbackMap[baseRequest.cid];
						baseRequest.callback = null;
						clearTimeout(baseRequest.timeoutId);
						delete baseRequest.timeoutId;
						reject(new TimeoutError(`Method '${[service, baseRequest.method].filter(Boolean).join('.')}' timed out.`));
					},
					baseRequest.ackTimeoutMs
				);
			}

			abort = () => {
				delete callbackMap[baseRequest.cid];

				if (baseRequest.timeoutId) {
					clearTimeout(baseRequest.timeoutId);
					delete baseRequest.timeoutId;
				}

				if (baseRequest.callback) {
					baseRequest.callback = null;
					reject(new AbortError(`Method '${[service, baseRequest.method].filter(Boolean).join('.')}' was aborted.`));
				}
			};

			baseRequest.callback = (err: Error | null, result?: unknown) => {
				delete callbackMap[baseRequest.cid];
				baseRequest.callback = null;

				if (baseRequest.timeoutId) {
					clearTimeout(baseRequest.timeoutId);
					delete baseRequest.timeoutId;
				}

				if (err) {
					reject(err);
					return;
				}

				resolve(result);
			};

			baseRequest.sentCallback = () => {
				delete baseRequest.sentCallback;
				this._outboundSentMessageCount++;
			};
		});

		this._outboundPreparedMessageCount++;

		const request = Object.assign(baseRequest, { promise });

		this.onInvoke(request as AnyRequest);

		return [promise, abort!];
	}

	protected onClose(code: number, reason?: Buffer | string): void {
		const prevStatus = this.status;
		this.webSocket = null;
		this._isReady = false;

		if (this._pingTimeoutRef) {
			clearTimeout(this._pingTimeoutRef);

			this._pingTimeoutRef = null;
		}

		this.abortAllPendingCallbacksDueToBadConnection(prevStatus);

		for (const plugin of this.plugins) {
			if (plugin.onClose) {
				plugin.onClose({ socket: this.socket, transport: this });
			}
		}

		if (!SocketProtocolIgnoreStatuses[code]) {
			let closeMessage: string;

			if (typeof reason === 'string') {
				closeMessage = `Socket connection closed with status code ${code} and reason: ${reason}`;
			} else {
				closeMessage = `Socket connection closed with status code ${code}`;
			}

			this.onError(new SocketProtocolError(SocketProtocolErrorStatuses[code] || closeMessage, code));
		}

		const strReason = reason?.toString() || SocketProtocolErrorStatuses[code];

		this._socket.emit('close', { code, reason: strReason });
	}

	protected onDisconnect(status: SocketStatus, code: number, reason?: string): void {
		if (status === 'ready') {
			this._socket.emit('disconnect', { code, reason });
		} else {
			this._socket.emit('connectAbort', { code, reason });
		}

		for (const plugin of this.plugins) {
			if (plugin.onDisconnected) {
				plugin.onDisconnected({ code, reason, socket: this.socket, status, transport: this });
			}
		}
	}

	public onError(error: Error): void {
		this._socket.emit('error', { error });
	}

	protected onInvoke(request: AnyRequest): void {
		this.sendRequest([request]);
	}

	protected onMessage(data: ws.Data, isBinary: boolean): void {
		data = isBinary ? data : data.toString();

		if (data === '') {
			this.onPingPong();
			return;
		}

		const timestamp = new Date();
		let p = Promise.resolve(data);
		let resolve: (() => void) | undefined;
		let reject: (err: Error) => void;

		const promise = new Promise<void>((res, rej) => {
			resolve = res;
			reject = rej;
		});

		this._inboundReceivedMessageCount++;

		for (let i = 0; i < this.plugins.length; i++) {
			const plugin = this.plugins[i];

			if (plugin?.onMessageRaw) {
				p = p.then((message) => {
					return plugin.onMessageRaw!({ message, promise, socket: this.socket, timestamp, transport: this });
				});
			}
		}

		p.then((data) => {
			const packet:
				(AnyPacket | AnyResponse)[]
				| (AnyPacket | AnyResponse)
				| null = this.decode(data);

			this._socket.emit('message', { data, isBinary });
			return this.handleInboudMessage({ packet, timestamp });
		})
			.then(resolve)
			.catch((err) => {
				reject(err);
				if (!(err instanceof PluginBlockedError)) {
					this.onError(err);
				}
			}).finally(() => {
				this._inboundProcessedMessageCount++;
			});
	}

	protected onOpen(): void {
		// Placeholder for inherited classes
		for (const plugin of this.plugins) {
			if (plugin.onOpen) {
				plugin.onOpen({ socket: this.socket, transport: this });
			}
		}
	}

	protected onPingPong(): void {}

	protected async onRequest(packet: AnyPacket, timestamp: Date, pluginError?: Error): Promise<boolean> {
		this._socket.emit('request', { request: packet });

		const timeoutAt = typeof packet.ackTimeoutMs === 'number' ? new Date(timestamp.valueOf() + packet.ackTimeoutMs) : undefined;
		let wasHandled = false;

		let response: AnyResponse | undefined;
		let error: Error | undefined;

		if (pluginError) {
			wasHandled = true;
			error = pluginError;

			if (packet.cid) {
				response = { error: pluginError, rid: packet.cid, timeoutAt };
			}
		} else {
			const handler = this._handlers[packet.method as keyof TIncoming];

			if (handler) {
				wasHandled = true;

				try {
					const data = await handler(
						new RequestHandlerArgs({
							isRpc: !!packet.cid,
							method: packet.method,
							options: packet.data,
							socket: this._socket,
							timeoutMs: packet.ackTimeoutMs,
							transport: this
						})
					);

					if (packet.cid) {
						response = { data, rid: packet.cid, timeoutAt } as DataResponse;
					}
				} catch (err) {
					error = toError(err);

					if (packet.cid) {
						response = { error, rid: packet.cid, timeoutAt };
					}
				}
			}
		}

		if (response) {
			this.sendResponse([response]);
		}

		if (error) {
			this.onError(error);
		}

		if (!wasHandled) {
			wasHandled = this.onUnhandledRequest(packet);
		}

		return wasHandled;
	}

	protected onResponse(response: AnyResponse, pluginError?: Error) {
		const map = this._callbackMap[response.rid];

		if (map) {
			if (map.timeoutId) {
				clearTimeout(map.timeoutId);
				delete map.timeoutId;
			}

			if (pluginError) {
				map.callback(pluginError);
			} else if ('error' in response) {
				map.callback(hydrateError(response.error));
			} else {
				map.callback(null, 'data' in response ? response.data : undefined);
			}
		}

		if (pluginError) {
			this._socket.emit('response', { response: { error: pluginError, rid: response.rid } });
		} else {
			this._socket.emit('response', { response });
		}
	}

	private onSocketClose(event: ws.CloseEvent): void {
		this.onClose(event.code, event.reason);
	}

	private onSocketError(event: ws.ErrorEvent): void {
		this.onError(event.error);
	}

	private onSocketMessage(event: ws.MessageEvent): void {
		this.onMessage(event.data, false);
	}

	protected onTransmit(request: AnyRequest): void {
		this.sendRequest([request]);
	}

	protected onUnhandledRequest(_: AnyPacket): boolean {
		return false;
	}

	protected resetPingTimeout(timeoutMs: false | number, code: number) {
		if (this._pingTimeoutRef) {
			clearTimeout(this._pingTimeoutRef);
			this._pingTimeoutRef = null;
		}

		if (timeoutMs !== false) {
			// Use `WebSocket#terminate()`, which immediately destroys the connection,
			// instead of `WebSocket#close()`, which waits for the close timer.
			// Delay should be equal to the interval at which your server
			// sends out pings plus a conservative assumption of the latency.
			this._pingTimeoutRef = setTimeout(() => {
				if (this._webSocket) {
					this._webSocket.close(code);
				}
			}, timeoutMs);
		}
	}

	public send(data: Buffer | string): Promise<void> {
		return new Promise((resolve, reject) => {
			if (!this._webSocket) {
				reject(new SocketClosedError('Web socket is closed.'));
				return;
			}

			this._webSocket.send(
				data,
				(err) => {
					if (err) {
						reject(err);
						return;
					}

					resolve();
				}
			);
		});
	}

	protected sendRequest(requests: AnyRequest[]): void;
	protected sendRequest(index: number, requests: AnyRequest[]): void;
	protected sendRequest(index: AnyRequest[] | number, requests?: AnyRequest[]): void {
		if (typeof index === 'object') {
			requests = index;
			index = 0;
		}

		if (!requests) {
			return; // Shouldn't happen
		}

		// Filter out any requests that have already timed out.
		if (requests.some(request => isRequestDone(request))) {
			requests = requests.filter(req => isRequestDone(req));

			if (!requests.length) {
				return;
			}
		}

		for (; index < this.plugins.length; index++) {
			const plugin = this.plugins[index];

			if (plugin?.sendRequest) {
				index++;

				try {
					plugin.sendRequest({
						cont: this.sendRequest.bind(this, index),
						requests,
						socket: this.socket,
						transport: this
					});
				} catch (err) {
					const error = toError(err);

					for (const req of requests) {
						abortRequest(req, error);
					}
				}

				return;
			}
		}

		// If the socket is closed we need to call them back with an error.
		if (this.status === 'closed') {
			for (const req of requests) {
				const err = new BadConnectionError(
					`Socket ${'callback' in req ? 'invoke' : 'transmit'} ${String(req.method)} event was aborted due to a bad connection`,
					'connectAbort'
				);

				this.onError(err);

				abortRequest(req, err);
			}
			return;
		}

		const encode = requests.map((req) => {
			if ('callback' in req && req.callback) {
				const { callback, promise, timeoutId, ...rest } = req;

				this._callbackMap[req.cid] = {
					callback: req.callback as InvokeCallback<unknown>['callback'],
					method: ['service' in req ? req.service : '', req.method].filter(Boolean).join('.'),
					timeoutId: req.timeoutId
				};

				return rest;
			}

			const { promise, ...rest } = req;

			return rest;
		});

		let sendErr: undefined | WebSocketError;

		this.send(
			this.codecEngine.encode(encode.length === 1 ? encode[0] : encode)
		).catch((err) => {
			sendErr = err;
		}).then(() => {
			const errCode = sendErr?.code;

			for (const req of requests) {
				if (errCode === 'ECONNRESET') {
					sendErr = new BadConnectionError(
						`Socket ${'callback' in req ? 'invoke' : 'transmit'} ${String(req.method)} event was aborted due to a bad connection`,
						'connectAbort'
					);
				}

				if (req.sentCallback) {
					req.sentCallback(sendErr);
				}

				if (sendErr && 'callback' in req && req.callback) {
					req.callback(sendErr);
				}
			}
		}).catch((err) => {
			this.onError(err);
		});
	}

	protected sendResponse(responses: AnyResponse[]): void;
	protected sendResponse(index: number, responses: AnyResponse[]): void;
	protected sendResponse(index: AnyResponse[] | number, responses?: AnyResponse[]): void {
		if (typeof index === 'object') {
			responses = index;
			index = 0;
		}

		if (!responses) {
			return; // shouldn't happen
		}

		// Remove any response that has timed out
		if (!(responses = responses.filter(item => !item.timeoutAt || item.timeoutAt > new Date())).length) {
			return;
		}

		for (; index < this.plugins.length; index++) {
			const plugin = this.plugins[index];

			if (plugin && 'sendResponse' in plugin && plugin.sendResponse) {
				index++;

				try {
					plugin.sendResponse({
						cont: this.sendResponse.bind(this, index),
						responses,
						socket: this.socket,
						transport: this
					});
				} catch (err) {
					this.sendResponse(
						index, responses.map(item => ({ error: err, rid: item.rid, timeoutAt: item.timeoutAt }))
					);
				}

				return;
			}
		}

		// If the socket is closed we need to call them back with an error.
		if (this.status === 'closed') {
			for (const _ of responses) {
				this.onError(new Error(`WebSocket is not open: readyState 3 (CLOSED)`));
			}
			return;
		}

		for (const response of responses) {
			if ('error' in response) {
				response.error = dehydrateError(response.error);
			}

			delete response.timeoutAt;
		}

		// timeoutId?: NodeJS.Timeout;
		// callback: (err: Error, result?: U) => void | null
		this.send(
			this.codecEngine.encode(responses.length === 1 ? responses[0] : responses)
		).catch((err) => {
			this.onError(err);
		});
	}

	public async setAuthorization(signedAuthToken: SignedAuthToken, authToken?: AuthToken): Promise<boolean> {
		if (signedAuthToken !== this._signedAuthToken) {
			let newAuthToken: AuthToken | null | undefined = authToken;

			if (!newAuthToken) {
				const extractedAuthToken = extractAuthTokenData(signedAuthToken);

				if (typeof extractedAuthToken === 'string') {
					throw new InvalidArgumentsError('Invalid authToken.');
				}

				newAuthToken = extractedAuthToken;
			}

			this._authToken = newAuthToken;
			this._signedAuthToken = signedAuthToken;

			return true;
		}

		return false;
	}

	public setReadyStatus(pingTimeoutMs: number, authError?: Error): void {
		if (this._webSocket?.readyState !== ws.OPEN) {
			throw new InvalidActionError('Cannot set status to OPEN before socket is connected.');
		}

		this._isReady = true;

		for (const plugin of this.plugins) {
			if (plugin.onReady) {
				plugin.onReady({ socket: this.socket, transport: this });
			}
		}

		this._socket.emit('connect', { authError, id: this.id, isAuthenticated: !!this.signedAuthToken, pingTimeoutMs });
	}

	public get signedAuthToken(): null | SignedAuthToken {
		return this._signedAuthToken;
	}

	public get socket(): Socket<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
		return this._socket;
	}

	public set socket(value: Socket<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>) {
		this._socket = value;
	}

	public get status(): SocketStatus {
		if (!this._webSocket) {
			return 'closed';
		}

		if (this._isReady) {
			return 'ready';
		}

		switch (this._webSocket.readyState) {
			case ws.CLOSING:
				return 'closing';
			case ws.CONNECTING:
			case ws.OPEN:
				return 'connecting';

			default:
				return 'closed';
		}
	}

	public transmit(
		serviceAndMethod: [string, string] | string,
		arg?: unknown
	): Promise<void> {
		let serviceRequest: Omit<TransmitServiceRequest, 'promise'> | undefined;
		let methodRequest: Omit<TransmitMethodRequest, 'promise'> | undefined;

		if (Array.isArray(serviceAndMethod)) {
			serviceRequest = {
				data: arg,
				method: serviceAndMethod[1],
				service: serviceAndMethod[0]
			};
		} else {
			methodRequest = {
				data: arg,
				method: serviceAndMethod
			};
		}

		const baseRequest = (serviceRequest || methodRequest!);

		const promise = new Promise<void>((resolve, reject) => {
			baseRequest.sentCallback = (err?: Error) => {
				delete baseRequest.sentCallback;
				this._outboundSentMessageCount++;

				if (err) {
					reject(err);
					return;
				}

				resolve();
			};
		});

		this._outboundPreparedMessageCount++;

		const request = Object.assign(baseRequest, { promise });

		this.onTransmit(request as AnyRequest);

		return request.promise;
	}

	public triggerAuthenticationEvents(wasSigned: boolean, wasAuthenticated: boolean): void {
		if (!this._signedAuthToken) {
			throw new AuthError('Signed auth token should be set to trigger authentication events');
		}

		this._socket.emit(
			'authStateChange',
			{ authToken: this._authToken, isAuthenticated: true, signedAuthToken: this._signedAuthToken, wasAuthenticated }
		);

		this._socket.emit(
			'authenticate',
			{ authToken: this._authToken, signedAuthToken: this._signedAuthToken, wasSigned }
		);

		for (const plugin of this.plugins) {
			if (plugin.onAuthenticated) {
				plugin.onAuthenticated({ socket: this.socket, transport: this });
			}
		}
	}

	public get url(): string {
		return this._webSocket?.url || '';
	}

	protected get webSocket(): null | ws.WebSocket {
		return this._webSocket;
	}

	protected set webSocket(value: null | ws.WebSocket) {
		if (this._webSocket) {
			this._webSocket.onclose = null;
			this._webSocket.onerror = null;
			this._webSocket.onmessage = null;
			this._webSocket.onopen = null;
		}

		this._webSocket = value;

		if (this._webSocket) {
			this._webSocket.onclose = this.onSocketClose.bind(this);
			this._webSocket.onopen = this.onOpen.bind(this);
			this._webSocket.onerror = this.onSocketError.bind(this);
			this._webSocket.onmessage = this.onSocketMessage.bind(this);
		}
	}
}
