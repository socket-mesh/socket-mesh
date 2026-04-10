import { ChannelMap, SubscribeEvent, SubscribeFailEvent, SubscribeStateChangeEvent, UnsubscribeEvent } from '@socket-mesh/channels';
import { ClientPrivateMap, ClientSocket, ServerPrivateMap } from '@socket-mesh/client';
import {
	AuthenticateEvent, AuthStateChangeEvent, BadAuthTokenEvent, ConnectEvent, ConnectingEvent, DeauthenticateEvent,
	DisconnectEvent, MessageEvent, PingEvent, PongEvent, PrivateMethodMap, PublicMethodMap,
	RemoveAuthTokenEvent,
	CloseEvent as SCloseEvent,
	ErrorEvent as SErrorEvent,
	ServiceMap, TypedRequestEvent, TypedResponseEvent
} from '@socket-mesh/core';
import { IncomingMessage } from 'http';

import { ServerSocket } from '../server-socket.js';

export interface CloseEvent {}

export interface ConnectionEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
	upgradeReq: IncomingMessage
}

export interface ErrorEvent {
	error: Error
}

export interface HandshakeEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
}

export interface HeadersEvent {
	headers: string[],
	request: IncomingMessage
}

export interface ListeningEvent {}

export type ServerEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> =
	CloseEvent
	| ConnectionEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| ErrorEvent
	| HandshakeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| HeadersEvent
	| ListeningEvent
	| SocketAuthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketAuthStateChangeEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketBadAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketCloseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketConnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketConnectingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketDeauthenticateEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketDisconnectEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketErrorEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketMessageEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketPingEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketPongEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketRemoveAuthTokenEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| SocketRequestEvent<TIncoming, TService, TPrivateIncoming>
	| SocketResponseEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	| WarningEvent;

export interface ServerSocketEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	socket:
		ClientSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateOutgoing>
		| ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
}

export type SocketAuthenticateEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = AuthenticateEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketAuthStateChangeEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = AuthStateChangeEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketBadAuthTokenEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = BadAuthTokenEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketCloseEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = SCloseEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketConnectEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = ConnectEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketConnectingEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = ConnectingEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketDeauthenticateEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = DeauthenticateEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketDisconnectEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = DisconnectEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketErrorEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = SErrorEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketMessageEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = MessageEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketPingEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = PingEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketPongEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = PongEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketRemoveAuthTokenEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = RemoveAuthTokenEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketRequestEvent<
	TIncoming extends PublicMethodMap,
	TService extends ServiceMap,
	TPrivateIncoming extends PrivateMethodMap
> = TypedRequestEvent<TIncoming & TPrivateIncoming & ServerPrivateMap, TService>;

export type SocketResponseEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = TypedResponseEvent<TOutgoing, TPrivateOutgoing & ClientPrivateMap, TService>
	& ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketSubscribeEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = SubscribeEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketSubscribeFailEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = SubscribeFailEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketSubscribeStateChangeEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = SubscribeStateChangeEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export type SocketUnsubscribeEvent<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> = UnsubscribeEvent & ServerSocketEvent<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;

export interface WarningEvent {
	warning: Error
}
