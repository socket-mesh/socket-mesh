import { AuthEngine, AuthOptions } from '@socket-mesh/auth-engine';
import { ChannelMap } from '@socket-mesh/channels';
import { ServerPrivateMap } from '@socket-mesh/client';
import { CallIdGenerator, HandlerMap, PrivateMethodMap, PublicMethodMap, ServiceMap, StreamCleanupMode } from '@socket-mesh/core';
import { CodecEngine } from '@socket-mesh/formatter';
import { ServerOptions as WebSocketServerOptions } from 'ws';

import { Broker } from './broker/broker.js';
import { ServerPlugin } from './plugin/server-plugin.js';
import { ServerSocketState } from './server-socket-state.js';
import { ServerSocket } from './server-socket.js';
import { ServerTransport } from './server-transport.js';

export interface ServerOptions<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
> extends WebSocketServerOptions {
	// In milliseconds, the timeout for receiving a response
	// when using invoke() or invokePublish().
	ackTimeoutMs?: number,

	// Whether or not clients are allowed to publish messages to channels.
	allowClientPublish?: boolean,

	authEngine?: AuthEngine | AuthOptions,

	brokerEngine?: Broker<TChannel>,

	callIdGenerator?: CallIdGenerator,

	codecEngine?: CodecEngine,

	handlers?: HandlerMap<
		TIncoming & TPrivateIncoming & ServerPrivateMap,
		TState & ServerSocketState,
		ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
		ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
	>,

	// In milliseconds, the timeout for receiving a response
	// when using invoke() or invokePublish(). ackTimeout: number
	handshakeTimeoutMs?: number,

	isPingTimeoutDisabled?: boolean,

	// Origins which are allowed to connect to the server.
	origins?: string,

	// The interval in milliseconds on which to send a ping to the client to check that
	// it is still alive.
	pingIntervalMs?: number,

	pingTimeoutMs?: number,

	plugins?: ServerPlugin<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>[],

	// Handler groups keyed by service name. Each key must match a service
	// declared in TService and the handlers are strongly-typed against that
	// service's method map. Services can also be added/removed dynamically
	// at runtime via Server.addHandlers()/removeHandlers().
	serviceHandlers?: {
		[TServiceName in keyof TService & string]?: HandlerMap<
			TService[TServiceName],
			TState & ServerSocketState,
			ServerSocket<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>,
			ServerTransport<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
		>
	},

	// The maximum number of unique channels which a single socket can subscribe to.
	socketChannelLimit?: number,

	// Lets you specify the default cleanup behaviour for
	// when a socket becomes disconnected.
	// Can be either 'kill' or 'close'. Kill mode means
	// that all of the socket's streams will be killed and
	// so consumption will stop immediately.
	// Close mode means that consumers on the socket will
	// be able to finish processing their stream backlogs
	// bebfore they are ended.
	socketStreamCleanupMode?: StreamCleanupMode,

	strictHandshake?: boolean
}
