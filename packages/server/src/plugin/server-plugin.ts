import { ChannelMap, ChannelOptions } from '@socket-mesh/channels';
import { Plugin, PrivateMethodMap, PublicMethodMap, ServiceMap } from '@socket-mesh/core';
import { IncomingMessage } from 'http';

import { AuthInfo } from '../handlers/authenticate.js';
import { ServerSocket } from '../server-socket.js';
import { ServerTransport } from '../server-transport.js';

export interface HandshakePluginArgs<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	authInfo: AuthInfo,
	socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
	transport: ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
}

export interface PublishPluginArgs<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	channel: string,
	data: any,
	socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
	transport: ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
}

export interface ServerPlugin<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> extends Plugin<
		ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
		ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	> {
	onAuthenticate?: (authInfo: AuthInfo) => void,
	onConnection?: (request: IncomingMessage) => Promise<void>,
	onHandshake?: (options: HandshakePluginArgs<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) => Promise<void>,
	onPublishIn?: (options: PublishPluginArgs<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) => Promise<any>,
	onPublishOut?: (options: PublishPluginArgs<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) => Promise<any>,
	onSubscribe?: (options: SubscribePluginArgs<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>) => Promise<void>
}

export interface SubscribePluginArgs<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
> {
	channel: string,
	options: ChannelOptions,
	socket: ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
	transport: ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
};
