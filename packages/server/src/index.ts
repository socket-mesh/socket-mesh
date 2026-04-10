import { ChannelMap } from '@socket-mesh/channels';
import { PrivateMethodMap, PublicMethodMap, ServiceMap } from '@socket-mesh/core';
import http from 'http';

import { ServerOptions } from './server-options.js';
import { Server } from './server.js';
export type { ServerRequestHandlerArgs } from './handlers/server-request-handler.js';
export type { ServerOptions } from './server-options.js';
export type { ServerSocketState } from './server-socket-state.js';
export { ServerSocket } from './server-socket.js';
export { Server } from './server.js';
export type { PluginType } from '@socket-mesh/core';

/**
 * Captures upgrade requests for a http.Server.
 *
 * @param {http.Server} server
 * @param {Object} options
 * @return {AGServer} websocket cluster server
 * @api public
 */
export function attach<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
>(
	server: http.Server,
	options?: ServerOptions<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
): Server<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState> {
	if (options == null) {
		options = {};
	}
	options.server = server;

	return new Server(options);
}
/**
 * Creates an http.Server exclusively used for WS upgrades.
 *
 * @param {Number} port
 * @param {Function} callback
 * @param {Object} options
 * @return {AGServer} websocket cluster server
 * @api public
 */
export function listen<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
>(): Server<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;
export function listen<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
>(
	port: number,
	options: ServerOptions<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>
): Server<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;
export function listen<
	TIncoming extends PublicMethodMap = {},
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = {},
	TService extends ServiceMap = {},
	TState extends object = {},
	TPrivateIncoming extends PrivateMethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {},
	TServerState extends object = {}
>(
	port: number,
	options: ServerOptions<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>, fn: () => void
): Server<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>;
export function listen<
	TIncoming extends PublicMethodMap,
	TOutgoing extends PublicMethodMap,
	TChannel extends ChannelMap,
	TService extends ServiceMap,
	TState extends object,
	TPrivateIncoming extends PrivateMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TServerState extends object
>(
	port?: number,
	options?: (() => void) | ServerOptions<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState>, fn?: () => void
): Server<TIncoming, TOutgoing, TChannel, TService, TState, TPrivateIncoming, TPrivateOutgoing, TServerState> {
	if (typeof options === 'function') {
		fn = options;
		options = {};
	} else if (!options) {
		options = {};
	}

	const server = http.createServer((req, res) => {
		res.writeHead(501);
		res.end('Not Implemented');
	});

	options.server = server;

	const socketClusterServer = attach(server, options);

	server.listen(port, fn);

	return socketClusterServer;
};
