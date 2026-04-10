import { SignedAuthToken } from '@socket-mesh/auth';
import { ChannelMap } from '@socket-mesh/channels';
import {
	AuthenticateEvent, AuthStateChangeEvent, BadAuthTokenEvent, BaseSocket, CloseEvent,
	ConnectEvent, ConnectingEvent, DeauthenticateEvent, DisconnectEvent, ErrorEvent,
	FunctionReturnType, InvokeMethodOptions, InvokeServiceOptions, MessageEvent,
	PingEvent, PongEvent, PrivateMethodMap, PublicMethodMap, RemoveAuthTokenEvent, RequestEvent,
	ResponseEvent, ServiceMap, ServiceMethodName, ServiceName, Socket, SocketEvent, toError,
	TypedRequestEvent, TypedResponseEvent, TypedSocketEvent, wait
} from '@socket-mesh/core';
import { hydrateError } from '@socket-mesh/errors';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';

import { ClientChannels } from './client-channels.js';
import { AutoReconnectOptions, ClientSocketOptions, ConnectOptions, parseClientOptions } from './client-socket-options.js';
import { ClientTransport } from './client-transport.js';
import { kickOutHandler } from './handlers/kickout.js';
import { publishHandler } from './handlers/publish.js';
import { removeAuthTokenHandler } from './handlers/remove-auth-token.js';
import { setAuthTokenHandler } from './handlers/set-auth-token.js';
import { ClientPrivateMap } from './maps/client-map.js';
import { ServerPrivateMap } from './maps/server-map.js';

export class ClientSocket<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = ChannelMap,
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateOutgoing extends PrivateMethodMap = {}
> extends BaseSocket<TState> implements Socket<
	TIncoming & ClientPrivateMap,
	TOutgoing,
	TState,
	TService,
	{},
	TPrivateOutgoing & ServerPrivateMap
> {
	private readonly _clientTransport: ClientTransport<TState>;
	public readonly channels: ClientChannels<TChannel, TState>;

	constructor(address: string | URL);
	constructor(options: ClientSocketOptions<TState>);
	constructor(options: ClientSocketOptions<TState> | string | URL) {
		options = parseClientOptions(options);

		options.handlers =
			Object.assign(
				{
					'#kickOut': kickOutHandler,
					'#publish': publishHandler,
					'#removeAuthToken': removeAuthTokenHandler,
					'#setAuthToken': setAuthTokenHandler
				},
				options.handlers
			);

		const clientTransport = new ClientTransport(options);

		super(clientTransport, options);

		this._clientTransport = clientTransport;
		this.channels = new ClientChannels<TChannel, TState>(this._clientTransport, options);

		if (options.autoConnect !== false) {
			this.connect(options);
		}
	}

	public async authenticate(signedAuthToken: SignedAuthToken): Promise<void> {
		try {
			await this._clientTransport.invoke('#authenticate', signedAuthToken)[0];

			this._clientTransport.setAuthorization(signedAuthToken);

			// In order for the events to trigger we need to wait for the next tick.
			await wait(0);
		} catch (err) {
			const error = toError(err);

			if (error.name !== 'BadConnectionError' && error.name !== 'TimeoutError') {
				// In case of a bad/closed connection or a timeout, we maintain the last
				// known auth state since those errors don't mean that the token is invalid.
				await this._clientTransport.changeToUnauthenticatedState();

				// In order for the events to trigger we need to wait for the next tick.
				await wait(0);
			}

			throw hydrateError(error);
		}
	}

	public get autoReconnect(): AutoReconnectOptions | false {
		return this._clientTransport.autoReconnect;
	}

	public set autoReconnect(value: boolean | Partial<AutoReconnectOptions>) {
		this._clientTransport.autoReconnect = value;
	}

	public connect(options?: ConnectOptions): void {
		this._clientTransport.connect(options);
	}

	public get connectTimeoutMs(): number {
		return this._clientTransport.connectTimeoutMs;
	}

	public set connectTimeoutMs(timeoutMs: number) {
		this._clientTransport.connectTimeoutMs = timeoutMs;
	}

	async deauthenticate(): Promise<boolean> {
		(async () => {
			let oldAuthToken: null | SignedAuthToken;

			try {
				oldAuthToken = await this._clientTransport.authEngine.removeToken();
			} catch (err) {
				this._clientTransport.onError(toError(err));
				return;
			}

			if (oldAuthToken) {
				this.emit('removeAuthToken', { oldAuthToken });
			}
		})();

		if (this.status !== 'closed') {
			await this._clientTransport.transmit('#removeAuthToken');
		}

		return await super.deauthenticate();
	}

	override emit(event: 'request', data: TypedRequestEvent<TIncoming & ClientPrivateMap, TService>): void;
	override emit(event: 'response', data: TypedResponseEvent<TOutgoing, TPrivateOutgoing & ServerPrivateMap, TService>): void;
	override emit(event: 'authStateChange', data: AuthStateChangeEvent): void;
	override emit(event: 'authenticate', data: AuthenticateEvent): void;
	override emit(event: 'badAuthToken', data: BadAuthTokenEvent): void;
	override emit(event: 'close', data: CloseEvent): void;
	override emit(event: 'connect', data: ConnectEvent): void;
	override emit(event: 'connectAbort', data: DisconnectEvent): void;
	override emit(event: 'connecting', data: ConnectingEvent): void;
	override emit(event: 'deauthenticate', data: DeauthenticateEvent): void;
	override emit(event: 'disconnect', data: DisconnectEvent): void;
	override emit(event: 'end'): void;
	override emit(event: 'error', data: ErrorEvent): void;
	override emit(event: 'message', data: MessageEvent): void;
	override emit(event: 'ping', data: PingEvent): void;
	override emit(event: 'pong', data: PongEvent): void;
	override emit(event: 'removeAuthToken', data: RemoveAuthTokenEvent): void;
	override emit(event: 'request', data: RequestEvent): void;
	override emit(event: 'response', data: ResponseEvent): void;
	override emit(event: string, data?: SocketEvent): void {
		(super.emit as (event: string, data?: SocketEvent) => void)(event, data);
	}

	override invoke<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod, (false | number)?], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: InvokeServiceOptions<TService, TServiceName, TMethod>, arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TMethod extends keyof TOutgoing & string>(options: InvokeMethodOptions<TOutgoing, TMethod>, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke(
		methodOptions: [string, string, (false | number)?] | InvokeMethodOptions | InvokeServiceOptions | string,
		arg?: unknown
	): Promise<unknown> {
		return super.invoke(methodOptions, arg);
	}

	public get isPingTimeoutDisabled(): boolean {
		return this._clientTransport.isPingTimeoutDisabled;
	}

	public set isPingTimeoutDisabled(isDisabled: boolean) {
		this._clientTransport.isPingTimeoutDisabled = isDisabled;
	}

	override listen(): DemuxedConsumableStream<StreamEvent<TypedSocketEvent<TIncoming & ClientPrivateMap, TOutgoing, TPrivateOutgoing & ServerPrivateMap, TService>>>;
	override listen(event: 'authStateChange'): DemuxedConsumableStream<AuthStateChangeEvent>;
	override listen(event: 'authenticate'): DemuxedConsumableStream<AuthenticateEvent>;
	override listen(event: 'badAuthToken'): DemuxedConsumableStream<BadAuthTokenEvent>;
	override listen(event: 'close'): DemuxedConsumableStream<CloseEvent>;
	override listen(event: 'connect'): DemuxedConsumableStream<ConnectEvent>;
	override listen(event: 'connectAbort'): DemuxedConsumableStream<DisconnectEvent>;
	override listen(event: 'connecting'): DemuxedConsumableStream<ConnectingEvent>;
	override listen(event: 'deauthenticate'): DemuxedConsumableStream<DeauthenticateEvent>;
	override listen(event: 'disconnect'): DemuxedConsumableStream<DisconnectEvent>;
	override listen(event: 'end'): DemuxedConsumableStream<void>;
	override listen(event: 'error'): DemuxedConsumableStream<ErrorEvent>;
	override listen(event: 'message'): DemuxedConsumableStream<MessageEvent>;
	override listen(event: 'ping'): DemuxedConsumableStream<PingEvent>;
	override listen(event: 'pong'): DemuxedConsumableStream<PongEvent>;
	override listen(event: 'removeAuthToken'): DemuxedConsumableStream<RemoveAuthTokenEvent>;
	override listen(event: 'request'): DemuxedConsumableStream<TypedRequestEvent<TIncoming & ClientPrivateMap, TService>>;
	override listen(event: 'response'): DemuxedConsumableStream<TypedResponseEvent<TOutgoing, TPrivateOutgoing & ServerPrivateMap, TService>>;
	override listen<U extends TypedSocketEvent<TIncoming & ClientPrivateMap, TOutgoing, TPrivateOutgoing & ServerPrivateMap, TService>, V = U>(event: string): DemuxedConsumableStream<V>;
	override listen<U extends TypedSocketEvent<TIncoming & ClientPrivateMap, TOutgoing, TPrivateOutgoing & ServerPrivateMap, TService>, V = U>(event?: string): DemuxedConsumableStream<V> {
		return super.listen<U, V>(event ?? '');
	}

	public get pingTimeoutMs(): number {
		return this._clientTransport.pingTimeoutMs;
	}

	public set pingTimeoutMs(timeoutMs: number) {
		this._clientTransport.pingTimeoutMs = timeoutMs;
	}

	public reconnect(code?: number, reason?: string) {
		this.disconnect(code, reason);
		this.connect();
	}

	override transmit<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<void>;
	override transmit<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<void>;
	override transmit(
		serviceAndMethod: [string, string] | string,
		arg?: unknown
	): Promise<void> {
		return super.transmit(serviceAndMethod, arg);
	}

	get type(): 'client' {
		return this._clientTransport.type;
	}

	public get uri(): URL {
		return this._clientTransport.uri;
	}
}
