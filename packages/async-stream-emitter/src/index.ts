import { DemuxedConsumableStream, StreamDemux, StreamDemuxStats, StreamEvent } from '@socket-mesh/stream-demux';

export interface EventEmitter {
	emit(eventName: string | symbol, ...args: any[]): boolean,

	listenerCount?(eventName: string | symbol, listener?: (...args: any[]) => void): number,

	on?(eventName: string | symbol, listener: (...args: any[]) => void): this
}

export class AsyncStreamEmitter<T> {
	private _listenerDemux: StreamDemux<T>;

	constructor() {
		this._listenerDemux = new StreamDemux<T>();
	}

	closeListeners(eventName?: string): void {
		if (eventName === undefined) {
			this._listenerDemux.closeAll();
			return;
		}

		this._listenerDemux.close(eventName);
	}

	emit(eventName: string, data: T): void {
		this._listenerDemux.write(eventName, data);
	}

	static from<T>(object: EventEmitter): AsyncStreamEmitter<T> {
		const result = new AsyncStreamEmitter<T>();

		const objEmitMethod = object.emit.bind(object) as (eventName: string | symbol, ...args: any[]) => boolean;
		const resultEmitMethod = result.emit.bind(result) as (eventName: string, data: T) => void;

		object.emit = (eventName: string, ...args: any[]): boolean => {
			const result = objEmitMethod.call(null, eventName, ...args);
			resultEmitMethod.call(null, eventName, args as T);

			return result;
		};

		// Prevent EventEmitter from throwing on error.
		if (object.on) {
			object.on('error', () => {});
		}

		if (object.listenerCount) {
			const objListenerCountMethod = object.listenerCount.bind(object) as (eventName: string | symbol, listener?: (...args: any[]) => void) => number;

			object.listenerCount = (eventName: string | symbol, listener?: (...args: any[]) => void): number => {
				const eventListenerCount = objListenerCountMethod(eventName, listener);
				const streamConsumerCount = result.getListenerConsumerCount(eventName as string);

				return eventListenerCount + streamConsumerCount;
			};
		}

		return result;
	}

	getListenerBackpressure(eventName?: string): number;
	getListenerBackpressure(consumerId?: number): number;
	getListenerBackpressure(eventName?: number | string): number {
		return this._listenerDemux.getBackpressure(eventName as any);
	}

	getListenerConsumerCount(eventName?: string): number {
		return this._listenerDemux.getConsumerCount(eventName);
	}

	getListenerConsumerStats(eventName?: string): StreamDemuxStats[];
	getListenerConsumerStats(consumerId?: number): StreamDemuxStats;
	getListenerConsumerStats(consumerId?: number | string): StreamDemuxStats | StreamDemuxStats[] {
		return this._listenerDemux.getConsumerStats(consumerId as any);
	}

	hasListenerConsumer(consumerId: number): boolean;
	hasListenerConsumer(eventName: string, consumerId: number): boolean;
	hasListenerConsumer(eventName: number | string, consumerId?: number) {
		return this._listenerDemux.hasConsumer(eventName as any, consumerId!);
	}

	killListeners(consumerId?: number): void;
	killListeners(eventName?: string): void;
	killListeners(eventName?: number | string): void {
		if (eventName === undefined) {
			this._listenerDemux.killAll();
			return;
		}

		this._listenerDemux.kill(eventName as any);
	}

	listen(): DemuxedConsumableStream<StreamEvent<T>>;
	listen<U extends T, V = U>(eventName: string): DemuxedConsumableStream<V>;
	listen<U extends T, V = U>(eventName?: string): DemuxedConsumableStream<StreamEvent<T>> | DemuxedConsumableStream<V> {
		return this._listenerDemux.listen<U, V>(eventName);
	}

	removeListener(eventName: string): void {
		this._listenerDemux.unlisten(eventName);
	}
}
