import { ChannelMap } from '@socket-mesh/channels';
import { ClientPrivateMap, ServerPrivateMap } from '@socket-mesh/client';
import {
	AuthenticateEvent, AuthStateChangeEvent, BadAuthTokenEvent, BaseSocket, BaseSocketOptions,
	CloseEvent, ConnectEvent, ConnectingEvent, DeauthenticateEvent, DisconnectEvent, ErrorEvent,
	FunctionReturnType, HandlerMap, InvokeMethodOptions, InvokeServiceOptions, MessageEvent,
	PingEvent, PongEvent, PrivateMethodMap, PublicMethodMap, RemoveAuthTokenEvent, RequestEvent,
	ResponseEvent, ServiceMap, ServiceMethodName, ServiceName, SocketEvent, toError,
	TypedRequestEvent, TypedResponseEvent, TypedSocketEvent
} from '@socket-mesh/core';
import { DemuxedConsumableStream, StreamEvent } from '@socket-mesh/stream-demux';
import { IncomingMessage } from 'http';
import { WebSocket } from 'ws';

import { Exchange } from './broker/exchange.js';
import { ServerPlugin } from './plugin/server-plugin.js';
import { ServerSocketState } from './server-socket-state.js';
import { ServerTransport } from './server-transport.js';
import { Server } from './server.js';

export interface ServerSocketOptions<
	TIncoming extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TOutgoing extends PublicMethodMap,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object,
	TState extends object
> extends BaseSocketOptions<TState & ServerSocketState> {
	handlers: HandlerMap<
		TIncoming & TPrivateIncoming & ServerPrivateMap,
		TState & ServerSocketState,
		ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
		ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
	>,
	id?: string,
	plugins?: ServerPlugin<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>[],
	request: IncomingMessage,
	server: Server<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
	service?: string,
	socket: WebSocket
}

export class ServerSocket<
	TIncoming extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TOutgoing extends PublicMethodMap = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {},
	TState extends object = {}
> extends BaseSocket<TState & ServerSocketState> {
	private _serverTransport: ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>;
	public readonly server: Server<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>;

	constructor(options: ServerSocketOptions<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) {
		const transport = new ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>(options);

		super(transport, options);

		this.server = options.server;
		this._serverTransport = transport;
	}

	async deauthenticate(rejectOnFailedDelivery?: boolean): Promise<boolean> {
		const result = await super.deauthenticate();

		if (rejectOnFailedDelivery) {
			try {
				await this._serverTransport.invoke('#removeAuthToken', undefined)[0];
			} catch (err) {
				const error = toError(err);

				this._serverTransport.onError(error);
				throw error;
			}
			return result;
		}

		try {
			await this.transmit('#removeAuthToken');
		} catch (err) {
			if (toError(err).name !== 'BadConnectionError') {
				throw err;
			}
		}

		return result;
	}

	override emit(event: 'request', data: TypedRequestEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TService>): void;
	override emit(event: 'response', data: TypedResponseEvent<TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>): void;
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

	public get exchange(): Exchange<TChannel> {
		return this.server.exchange;
	}

	public get id(): string {
		return this._serverTransport.id;
	}

	override invoke<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod, (false | number)?], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: InvokeServiceOptions<TService, TServiceName, TMethod>, arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TMethod extends keyof TOutgoing & string>(options: InvokeMethodOptions<TOutgoing, TMethod>, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke<TMethod extends keyof (TPrivateOutgoing & ClientPrivateMap) & string>(method: TMethod, arg: Parameters<(TPrivateOutgoing & ClientPrivateMap)[TMethod]>[0]): Promise<FunctionReturnType<(TPrivateOutgoing & ClientPrivateMap)[TMethod]>>;
	override invoke<TMethod extends keyof (TPrivateOutgoing & ClientPrivateMap) & string>(options: InvokeMethodOptions<(TPrivateOutgoing & ClientPrivateMap), TMethod>, arg?: Parameters<(TPrivateOutgoing & ClientPrivateMap)[TMethod]>[0]): Promise<FunctionReturnType<(TPrivateOutgoing & ClientPrivateMap)[TMethod]>>;
	override invoke(
		methodOptions: [string, string, (false | number)?] | InvokeMethodOptions | InvokeServiceOptions | string,
		arg?: unknown
	): Promise<unknown> {
		return super.invoke(methodOptions, arg);
	}

	kickOut(channel: string, message: string): Promise<void[]> {
		const channels = channel ? [channel] : Object.keys(this.state.channelSubscriptions || {});

		return Promise.all(channels.map((channelName) => {
			this.transmit('#kickOut', { channel: channelName, message });
			return this._serverTransport.unsubscribe(channelName);
		}));
	}

	override listen(): DemuxedConsumableStream<StreamEvent<TypedSocketEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>>>;
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
	override listen(event: 'request'): DemuxedConsumableStream<TypedRequestEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TService>>;
	override listen(event: 'response'): DemuxedConsumableStream<TypedResponseEvent<TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>>;
	override listen<U extends TypedSocketEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>, V = U>(event: string): DemuxedConsumableStream<V>;
	override listen<U extends TypedSocketEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>, V = U>(event?: string): DemuxedConsumableStream<V> {
		return super.listen<U, V>(event ?? '');
	}

	public ping(): Promise<void> {
		return this._serverTransport.ping();
	}

	get service(): string | undefined {
		return this._serverTransport.service;
	}

	override transmit<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<void>;
	override transmit<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<void>;
	override transmit<TMethod extends keyof (TPrivateOutgoing & ClientPrivateMap) & string>(method: TMethod, arg?: Parameters<(TPrivateOutgoing & ClientPrivateMap)[TMethod]>[0]): Promise<void>;
	override transmit(
		serviceAndMethod: [string, string] | string,
		arg?: unknown
	): Promise<void> {
		return super.transmit(serviceAndMethod, arg);
	}

	get type(): 'server' {
		return this._serverTransport.type;
	}
}
