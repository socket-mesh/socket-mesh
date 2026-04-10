import { ChannelMap } from '@socket-mesh/channels';
import { ClientPrivateMap, ServerPrivateMap } from '@socket-mesh/client';
import { FunctionReturnType, HandlerMap, InvokeMethodOptions, InvokeServiceOptions, PrivateMethodMap, PublicMethodMap, ServiceMap, ServiceMethodName, ServiceName, Socket, SocketOptions, toError } from '@socket-mesh/core';
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
> extends SocketOptions<
	TIncoming & TPrivateIncoming & ServerPrivateMap,
	TOutgoing,
	TPrivateOutgoing & ClientPrivateMap,
	TService,
	TState & ServerSocketState
	> {
	handlers: HandlerMap<
		TIncoming & TPrivateIncoming & ServerPrivateMap,
		TOutgoing,
		TPrivateOutgoing & ClientPrivateMap,
		TService,
		TState & ServerSocketState
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
> extends Socket<
	TIncoming & TPrivateIncoming & ServerPrivateMap,
	TOutgoing,
	TPrivateOutgoing & ClientPrivateMap,
	TService,
	TState & ServerSocketState
	> {
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
