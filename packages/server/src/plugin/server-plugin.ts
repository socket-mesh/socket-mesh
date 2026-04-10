import { ChannelMap, ChannelOptions } from '@socket-mesh/channels';
import { Plugin, PrivateMethodMap, PublicMethodMap, ServiceMap } from '@socket-mesh/core';
import { IncomingMessage } from 'http';

import { AuthInfo } from '../handlers/authenticate.js';
import { ServerSocket } from '../server-socket.js';
import { ServerTransport } from '../server-transport.js';

export interface HandshakePluginArgs<
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object,
	TState extends object
> {
	authInfo: AuthInfo,
	socket: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
	transport: ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
}

export interface PublishPluginArgs<
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object,
	TState extends object
> {
	channel: string,
	data: any,
	socket: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
	transport: ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
}

export interface ServerPlugin<
	TIncoming extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TOutgoing extends PublicMethodMap,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object,
	TState extends object
> extends Plugin<
		ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
		ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
	> {
	onAuthenticate?: (authInfo: AuthInfo) => void,
	onConnection?: (request: IncomingMessage) => Promise<void>,
	onHandshake?: (options: HandshakePluginArgs<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) => Promise<void>,
	onPublishIn?: (options: PublishPluginArgs<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) => Promise<any>,
	onPublishOut?: (options: PublishPluginArgs<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) => Promise<any>,
	onSubscribe?: (options: SubscribePluginArgs<TChannel, TService, TIncoming, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>) => Promise<void>
}

export interface SubscribePluginArgs<
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object,
	TState extends object
> {
	channel: string,
	options: ChannelOptions,
	socket: ServerSocket<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>,
	transport: ServerTransport<TIncoming, TChannel, TService, TOutgoing, TPrivateIncoming, TPrivateOutgoing, TServerState, TState>
};
