import { AsyncStreamEmitter } from '@socket-mesh/async-stream-emitter';
import { AuthToken, SignedAuthToken } from '@socket-mesh/auth';
import { CodecEngine } from '@socket-mesh/formatter';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';

import { HandlerMap } from './maps/handler-map.js';
import { FunctionReturnType, MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap, ServiceName } from './maps/method-map.js';
import { Plugin } from './plugins/plugin.js';
import { AuthenticateEvent, AuthStateChangeEvent, BadAuthTokenEvent, CloseEvent, ConnectEvent, ConnectingEvent, DeauthenticateEvent, DisconnectEvent, ErrorEvent, MessageEvent, PingEvent, PongEvent, RemoveAuthTokenEvent, RequestEvent, ResponseEvent, SocketEvent } from './socket-event.js';
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
> extends AsyncStreamEmitter<SocketEvent<TIncoming, TOutgoing, TPrivateOutgoing, TService> | undefined> {
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
	emit(event: 'request', data: RequestEvent<TIncoming, TService>): void;
	emit(event: 'response', data: ResponseEvent<TOutgoing, TPrivateOutgoing, TService>): void;
	emit(event: string, data?: SocketEvent<TIncoming, TOutgoing, TPrivateOutgoing, TService>): void {
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

	public invoke<TMethod extends keyof TOutgoing>(
		method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	public invoke<TServiceName extends ServiceName<TService>, TMethod extends keyof TService[TServiceName]>(
		options: [TServiceName, TMethod, (false | number)?], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	public invoke<TServiceName extends ServiceName<TService>, TMethod extends keyof TService[TServiceName]>(
		options: InvokeServiceOptions<TService, TServiceName, TMethod>, arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	public invoke<TMethod extends keyof TOutgoing>(
		options: InvokeMethodOptions<TOutgoing, TMethod>, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	public invoke<TServiceName extends ServiceName<TService>, TServiceMethod extends keyof TService[TServiceName], TMethod extends keyof TOutgoing>(
		methodOptions: [TServiceName, TServiceMethod, (false | number)?] | InvokeMethodOptions<TOutgoing, TMethod> | InvokeServiceOptions<TService, TServiceName, TServiceMethod> | TMethod,
		arg?: Parameters<TOutgoing[TMethod] | TService[TServiceName][TServiceMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod] | TService[TServiceName][TServiceMethod]>> {
		return this._transport.invoke(methodOptions as TMethod, arg)[0];
	}

	listen(): DemuxedConsumableStream<StreamEvent<SocketEvent<TIncoming, TOutgoing, TPrivateOutgoing, TService>>>;
	listen(event: 'authStateChange'): DemuxedConsumableStream<AuthStateChangeEvent>;
	listen(event: 'authenticate'): DemuxedConsumableStream<AuthenticateEvent>;
	listen(event: 'badAuthToken'): DemuxedConsumableStream<BadAuthTokenEvent>;
	listen(event: 'close'): DemuxedConsumableStream<CloseEvent>;
	listen(event: 'connect'): DemuxedConsumableStream<ConnectEvent>;
	listen(event: 'connectAbort'): DemuxedConsumableStream<DisconnectEvent>;
	listen(event: 'connecting'): DemuxedConsumableStream<ConnectingEvent>;
	listen(event: 'deauthenticate'): DemuxedConsumableStream<AuthenticateEvent>;
	listen(event: 'disconnect'): DemuxedConsumableStream<DisconnectEvent>;
	listen(event: 'end'): DemuxedConsumableStream<void>;
	listen(event: 'error'): DemuxedConsumableStream<ErrorEvent>;
	listen(event: 'message'): DemuxedConsumableStream<MessageEvent>;
	listen(event: 'ping'): DemuxedConsumableStream<PingEvent>;
	listen(event: 'pong'): DemuxedConsumableStream<PongEvent>;
	listen(event: 'removeAuthToken'): DemuxedConsumableStream<RemoveAuthTokenEvent>;
	listen(event: 'request'): DemuxedConsumableStream<RequestEvent<TIncoming, TService>>;
	listen(event: 'response'): DemuxedConsumableStream<ResponseEvent<TOutgoing, TPrivateOutgoing, TService>>;
	listen<U extends SocketEvent<TIncoming, TOutgoing, TPrivateOutgoing, TService>, V = U>(event: string): DemuxedConsumableStream<V>;
	listen<U extends SocketEvent<TIncoming, TOutgoing, TPrivateOutgoing, TService>, V = U>(event?: string): DemuxedConsumableStream<V> {
		return super.listen(event ?? '');
	}

	public get signedAuthToken(): null | SignedAuthToken {
		return this._transport.signedAuthToken;
	}

	public get status(): SocketStatus {
		return this._transport.status;
	}

	public transmit<TMethod extends keyof TOutgoing>(
		method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<void>;
	public transmit<TServiceName extends ServiceName<TService>, TMethod extends keyof TService[TServiceName]>(
		options: [TServiceName, TMethod], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<void>;
	public transmit<TServiceName extends ServiceName<TService>, TServiceMethod extends keyof TService[TServiceName], TMethod extends keyof TOutgoing>(
		serviceAndMethod: [TServiceName, TServiceMethod] | TMethod,
		arg?: (Parameters<TOutgoing[TMethod] | TService[TServiceName][TServiceMethod]>)[0]): Promise<void> {
		return this._transport.transmit(serviceAndMethod as TMethod, arg);
	}

	public get url(): string {
		return this._transport.url;
	}
}
