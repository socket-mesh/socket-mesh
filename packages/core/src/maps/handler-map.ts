import { RequestHandler } from '../request-handler.js';
import { BaseSocketTransport } from '../socket-transport.js';
import { BaseSocket } from '../socket.js';
import { MethodMap } from './method-map.js';

export type HandlerMap<
	TIncoming extends MethodMap,
	TState extends object,
	TSocket extends BaseSocket<TState> = BaseSocket<TState>,
	TTransport extends BaseSocketTransport<TState> = BaseSocketTransport<TState>
> = Partial<
	{
		[K in keyof TIncoming]:
		RequestHandler<
			Parameters<TIncoming[K]>[0],
			ReturnType<TIncoming[K]>,
			TState,
			TSocket,
			TTransport
		>
	}
>;
