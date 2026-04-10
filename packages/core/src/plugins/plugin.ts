import ws from 'isomorphic-ws';

import { AnyPacket } from '../packet.js';
import { LooseHandlerMap } from '../request-handler.js';
import { AnyRequest } from '../request.js';
import { AnyResponse } from '../response.js';
import { BaseSocketTransport } from '../socket-transport.js';
import { BaseSocket, SocketStatus } from '../socket.js';

export type AnyPlugin = Plugin<any, any>;

export interface DisconnectedPluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> extends PluginArgs<TSocket, TTransport> {
	code: number,
	reason?: string,
	status: SocketStatus
}

export interface MessagePluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> extends PluginArgs<TSocket, TTransport> {
	packet: AnyPacket | AnyResponse,
	timestamp: Date
}

export interface MessageRawPluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> extends PluginArgs<TSocket, TTransport> {
	message: string | ws.RawData,
	promise: Promise<void>,
	timestamp: Date
}

export interface Plugin<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> {
	handlers?: LooseHandlerMap,
	onAuthenticated?(options: PluginArgs<TSocket, TTransport>): void,
	onClose?(options: PluginArgs<TSocket, TTransport>): void,
	onDeauthenticate?(options: PluginArgs<TSocket, TTransport>): void,
	onDisconnected?(options: DisconnectedPluginArgs<TSocket, TTransport>): void,
	onEnd?(options: PluginArgs<TSocket, TTransport>): void,
	onMessage?(options: MessagePluginArgs<TSocket, TTransport>): Promise<AnyPacket | AnyResponse>,
	onMessageRaw?(options: MessageRawPluginArgs<TSocket, TTransport>): Promise<string | ws.RawData>,
	onOpen?(options: PluginArgs<TSocket, TTransport>): void,
	onReady?(options: PluginArgs<TSocket, TTransport>): void,
	sendRequest?(options: SendRequestPluginArgs<TSocket, TTransport>): void,
	sendResponse?(options: SendResponsePluginArgs<TSocket, TTransport>): void,
	type: string
}

export interface PluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> {
	socket: TSocket,
	transport: TTransport
}

export type PluginType = 'handshake' | 'request' | 'response';

export interface SendRequestPluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> extends PluginArgs<TSocket, TTransport> {
	cont: (requests: AnyRequest[]) => void,
	requests: AnyRequest[]
}

export interface SendResponsePluginArgs<
	TSocket extends BaseSocket = BaseSocket,
	TTransport extends BaseSocketTransport = BaseSocketTransport
> extends PluginArgs<TSocket, TTransport> {
	cont: (requests: AnyResponse[]) => void,
	responses: AnyResponse[]
}
