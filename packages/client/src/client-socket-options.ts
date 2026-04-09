import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap, SocketOptions } from '@socket-mesh/core';
import ws from 'isomorphic-ws';

import { ClientAuthEngine, LocalStorageAuthEngineOptions } from './client-auth-engine.js';
import { ClientPrivateMap } from './maps/client-map.js';
import { ServerPrivateMap } from './maps/server-map.js';

export interface AutoReconnectOptions {
	initialDelay: number,
	maxDelayMs: number,
	multiplier: number,
	randomness: number
}

export interface ClientSocketOptions<
	TOutgoing extends PublicMethodMap = {},
	TService extends ServiceMap = {},
	TIncoming extends MethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TState extends object = {}
> extends SocketOptions<
	TIncoming & ClientPrivateMap,
	TOutgoing,
	TPrivateOutgoing & ServerPrivateMap,
	TService,
	TState
	> {
	address: string | URL,

	// A custom engine to use for storing and loading JWT auth tokens on the client side.
	authEngine?: ClientAuthEngine | LocalStorageAuthEngineOptions | null,

	// Whether or not to automatically connect the socket as soon as it is created. Default is true.
	autoConnect?: boolean,

	// Whether or not to automatically reconnect the socket when it loses the connection. Default is true.
	// Valid properties are: initialDelay (milliseconds), randomness (milliseconds), multiplier (decimal; default is 1.5) and maxDelay (milliseconds).
	autoReconnect?: boolean | Partial<AutoReconnectOptions>,

	// This is true by default. If you set this to false, then the socket will not automatically try to subscribe to pending subscriptions on
	// connect - Instead, you will have to manually invoke the processSubscriptions callback from inside the 'connect' event handler on the client side.
	// See AGClientSocket API. This gives you more fine-grained control with regards to when pending subscriptions are processed after the socket
	// connection is established (or re-established).
	autoSubscribeOnConnect?: boolean,

	// A prefix to add to the channel names.
	channelPrefix?: string,

	connectTimeoutMs?: number,

	wsOptions?: ws.ClientOptions
}

export interface ConnectOptions {
	address?: string | URL,
	connectTimeoutMs?: number,
	wsOptions?: ws.ClientOptions
}

export function parseAutoReconnectOptions(value?: boolean | Partial<AutoReconnectOptions>): AutoReconnectOptions | false {
	if (value) {
		return {
			initialDelay: 10000,
			maxDelayMs: 60000,
			multiplier: 1.5,
			randomness: 10000,
			...(value === true ? {} : value)
		};
	}

	return false;
}

export function parseClientOptions<
	TIncoming extends MethodMap,
	TService extends ServiceMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TState extends object
>(options: ClientSocketOptions<TOutgoing, TService, TIncoming, TPrivateOutgoing, TState> | string | URL): ClientSocketOptions<TOutgoing, TService, TIncoming, TPrivateOutgoing, TState> {
	if (typeof options === 'string' || 'pathname' in options) {
		options = { address: options };
	}

	return { ...options };
}
