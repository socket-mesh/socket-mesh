import { ChannelMap } from '@socket-mesh/channels';
import { PrivateMethodMap, PublicMethodMap, RequestHandlerArgs, ServiceMap } from '@socket-mesh/core';

import { ServerSocketState } from '../server-socket-state.js';
import { ServerSocket } from '../server-socket.js';
import { ServerTransport } from '../server-transport.js';

export type ServerRequestHandlerArgs<
	TOptions,
	TIncoming extends PublicMethodMap = any,
	TOutgoing extends PublicMethodMap = any,
	TChannel extends ChannelMap = any,
	TService extends ServiceMap = any,
	TState extends object = any,
	TPrivateIncoming extends PrivateMethodMap = any,
	TPrivateOutgoing extends PrivateMethodMap = any,
	TServerState extends object = any
> =
	RequestHandlerArgs<
		TOptions,
		TState & ServerSocketState,
		ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
		ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	>;
