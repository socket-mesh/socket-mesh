import ws from 'isomorphic-ws';

import { HandlerMap } from '../maps/handler-map.js';
import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap } from '../maps/method-map.js';
import { AnyPacket } from '../packet.js';
import { AnyRequest } from '../request.js';
import { AnyResponse } from '../response.js';
import { SocketTransport } from '../socket-transport.js';
import { Socket, SocketStatus } from '../socket.js';

export interface DisconnectedPluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
	code: number,
	reason?: string,
	status: SocketStatus
}

export interface MessagePluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
	packet: AnyPacket | AnyResponse,
	timestamp: Date
}

export interface MessageRawPluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
	message: string | ws.RawData,
	promise: Promise<void>,
	timestamp: Date
}

export interface Plugin<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> {
	handlers?: HandlerMap<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>,
	onAuthenticated?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onClose?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onDeauthenticate?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onDisconnected?(options: DisconnectedPluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onEnd?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onMessage?(options: MessagePluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): Promise<AnyPacket | AnyResponse>,
	onMessageRaw?(options: MessageRawPluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): Promise<string | ws.RawData>,
	onOpen?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	onReady?(options: PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	sendRequest?(options: SendRequestPluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	sendResponse?(options: SendResponsePluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>): void,
	type: string
}

export interface PluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> {
	socket: Socket<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>,
	transport: SocketTransport<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState>
}

export type PluginType = 'handshake' | 'request' | 'response';

export interface SendRequestPluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
	cont: (requests: AnyRequest[]) => void,
	requests: AnyRequest[]
}

export interface SendResponsePluginArgs<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap,
	TState extends object
> extends PluginArgs<TIncoming, TOutgoing, TPrivateOutgoing, TService, TState> {
	cont: (requests: AnyResponse[]) => void,
	responses: AnyResponse[]
}
