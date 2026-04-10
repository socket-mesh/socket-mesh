import { TimeoutError } from '@socket-mesh/errors';

import { BaseSocketTransport } from './socket-transport.js';
import { BaseSocket } from './socket.js';

export interface LooseHandlerMap {
	[method: string]: ((args: RequestHandlerArgs<any, any, any, any>) => Promise<any>) | undefined
}

export type RequestHandler<
	TOptions, U,
	TState extends object,
	TSocket extends BaseSocket<TState> = BaseSocket<TState>,
	TTransport extends BaseSocketTransport<TState> = BaseSocketTransport<TState>
> = (args: RequestHandlerArgs<TOptions, TState, TSocket, TTransport>) => Promise<U>;

export interface RequestHandlerArgsOptions<
	TOptions,
	TState extends object,
	TSocket extends BaseSocket<TState> = BaseSocket<TState>,
	TTransport extends BaseSocketTransport<TState> = BaseSocketTransport<TState>
> {
	isRpc: boolean,
	method: string,
	options: TOptions,
	socket: TSocket,
	timeoutMs?: boolean | number,
	transport: TTransport
}

export class RequestHandlerArgs<
	TOptions,
	TState extends object = {},
	TSocket extends BaseSocket<TState> = BaseSocket<TState>,
	TTransport extends BaseSocketTransport<TState> = BaseSocketTransport<TState>
> {
	public isRpc: boolean;
	public method: string;
	public options: TOptions;
	public requestedAt: Date;
	public socket: TSocket;
	public timeoutMs?: boolean | number;
	public transport: TTransport;

	constructor(options: RequestHandlerArgsOptions<TOptions, TState, TSocket, TTransport>) {
		this.isRpc = options.isRpc;
		this.method = options.method;
		this.options = options.options;
		this.requestedAt = new Date();
		this.socket = options.socket;
		this.transport = options.transport;
		this.timeoutMs = options.timeoutMs;
	}

	checkTimeout(timeLeftMs = 0): void {
		if (typeof this.timeoutMs === 'number' && this.getRemainingTimeMs() <= timeLeftMs) {
			throw new TimeoutError(`Method '${this.method}' timed out.`);
		}
	}

	getRemainingTimeMs(): number {
		if (typeof this.timeoutMs === 'number') {
			return (this.requestedAt.valueOf() + this.timeoutMs) - new Date().valueOf();
		}

		return Infinity;
	}
}
