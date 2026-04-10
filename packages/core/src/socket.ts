import { AsyncStreamEmitter } from '@socket-mesh/async-stream-emitter';
import { AuthToken, SignedAuthToken } from '@socket-mesh/auth';
import { CodecEngine } from '@socket-mesh/formatter';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';

import { HandlerMap } from './maps/handler-map.js';
import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap } from './maps/method-map.js';
import { Plugin } from './plugins/plugin.js';
import {
	AuthenticateEvent, AuthStateChangeEvent, BadAuthTokenEvent, CloseEvent, ConnectEvent,
	ConnectingEvent, DeauthenticateEvent, DisconnectEvent, ErrorEvent, MessageEvent, PingEvent,
	PongEvent, RemoveAuthTokenEvent, RequestEvent, ResponseEvent, SocketEvent
} from './socket-event.js';
import { CallIdGenerator, InvokeMethodOptions, InvokeServiceOptions, SocketTransport } from './socket-transport.js';

export interface SocketOptions<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> {
	ackTimeoutMs?: number,
	callIdGenerator?: CallIdGenerator,
	codecEngine?: CodecEngine,
	handlers?: HandlerMap<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>,
	isPingTimeoutDisabled?: boolean,
	plugins?: Plugin<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>[],
	state?: Partial<TState>,

	// Lets you specify the default cleanup behaviour for
	// when a socket becomes disconnected.
	// Can be either 'kill' or 'close'. Kill mode means
	// that all of the socket's streams will be killed and
	// so consumption will stop immediately.
	// Close mode means that consumers on the socket will
	// be able to finish processing their stream backlogs
	// bebfore they are ended.
	streamCleanupMode?: StreamCleanupMode
}

export type SocketStatus = 'closed' | 'closing' | 'connecting' | 'ready';

export type StreamCleanupMode = 'close' | 'kill' | 'none';

export class Socket<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends AsyncStreamEmitter<SocketEvent | undefined> {
	private readonly _transport: SocketTransport<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>;
	public readonly state: Partial<TState>;

	protected constructor(
		transport: SocketTransport<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>,
		options?: SocketOptions<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>
	) {
		super();

		this.state = options?.state || {};
		transport.socket = this;
		this._transport = transport;
	}

	public get authToken(): AuthToken | null {
		return this._transport.authToken;
	}

	public deauthenticate(): Promise<boolean> {
		return this._transport.changeToUnauthenticatedState();
	}

	public disconnect(code = 1000, reason?: string): void {
		this._transport.disconnect(code, reason);
	}

	emit(event: 'authStateChange', data: AuthStateChangeEvent): void;
	emit(event: 'authenticate', data: AuthenticateEvent): void;
	emit(event: 'badAuthToken', data: BadAuthTokenEvent): void;
	emit(event: 'close', data: CloseEvent): void;
	emit(event: 'connect', data: ConnectEvent): void;
	emit(event: 'connectAbort', data: DisconnectEvent): void;
	emit(event: 'connecting', data: ConnectingEvent): void;
	emit(event: 'deauthenticate', data: DeauthenticateEvent): void;
	emit(event: 'disconnect', data: DisconnectEvent): void;
	emit(event: 'end'): void;
	emit(event: 'error', data: ErrorEvent): void;
	emit(event: 'message', data: MessageEvent): void;
	emit(event: 'ping', data: PingEvent): void;
	emit(event: 'pong', data: PongEvent): void;
	emit(event: 'removeAuthToken', data: RemoveAuthTokenEvent): void;
	emit(event: 'request', data: RequestEvent): void;
	emit(event: 'response', data: ResponseEvent): void;
	emit(event: string, data?: SocketEvent): void {
		super.emit(event, data);
	}

	public getBackpressure(): number {
		return Math.max(
			this._transport.getBackpressure(),
			this.getListenerBackpressure()
			// this.receiver.getBackpressure(),
			// this.procedure.getBackpressure()
		);
	}

	public getInboundBackpressure(): number {
		return this._transport.getInboundBackpressure();
	}

	public getOutboundBackpressure(): number {
		return this._transport.getOutboundBackpressure();
	}

	public get id(): null | string {
		return this._transport.id;
	}

	public invoke(
		methodOptions: [string, string, (false | number)?] | InvokeMethodOptions | InvokeServiceOptions | string,
		arg?: unknown
	): Promise<unknown> {
		return this._transport.invoke(methodOptions, arg)[0];
	}

	listen(): DemuxedConsumableStream<StreamEvent<SocketEvent>>;
	listen(event: 'authStateChange'): DemuxedConsumableStream<AuthStateChangeEvent>;
	listen(event: 'authenticate'): DemuxedConsumableStream<AuthenticateEvent>;
	listen(event: 'badAuthToken'): DemuxedConsumableStream<BadAuthTokenEvent>;
	listen(event: 'close'): DemuxedConsumableStream<CloseEvent>;
	listen(event: 'connect'): DemuxedConsumableStream<ConnectEvent>;
	listen(event: 'connectAbort'): DemuxedConsumableStream<DisconnectEvent>;
	listen(event: 'connecting'): DemuxedConsumableStream<ConnectingEvent>;
	listen(event: 'deauthenticate'): DemuxedConsumableStream<DeauthenticateEvent>;
	listen(event: 'disconnect'): DemuxedConsumableStream<DisconnectEvent>;
	listen(event: 'end'): DemuxedConsumableStream<void>;
	listen(event: 'error'): DemuxedConsumableStream<ErrorEvent>;
	listen(event: 'message'): DemuxedConsumableStream<MessageEvent>;
	listen(event: 'ping'): DemuxedConsumableStream<PingEvent>;
	listen(event: 'pong'): DemuxedConsumableStream<PongEvent>;
	listen(event: 'removeAuthToken'): DemuxedConsumableStream<RemoveAuthTokenEvent>;
	listen(event: 'request'): DemuxedConsumableStream<RequestEvent>;
	listen(event: 'response'): DemuxedConsumableStream<ResponseEvent>;
	listen<U extends SocketEvent, V = U>(event: string): DemuxedConsumableStream<V>;
	listen<U extends SocketEvent, V = U>(event?: string): DemuxedConsumableStream<V> {
		return super.listen(event ?? '');
	}

	public get signedAuthToken(): null | SignedAuthToken {
		return this._transport.signedAuthToken;
	}

	public get status(): SocketStatus {
		return this._transport.status;
	}

	public transmit(
		serviceAndMethod: [string, string] | string,
		arg?: unknown
	): Promise<void> {
		return this._transport.transmit(serviceAndMethod, arg);
	}

	public get url(): string {
		return this._transport.url;
	}
}
