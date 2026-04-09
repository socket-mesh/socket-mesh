import { ConsumerNode } from './consumer-node.js';
import { ConsumerStats } from './consumer-stats.js';
import { WritableConsumableStream } from './writable-consumable-stream.js';

function wait(timeout: number): { promise: Promise<void>, timeoutId: NodeJS.Timeout } {
	let timeoutId: NodeJS.Timeout | undefined = undefined;

	const promise: Promise<void> = new Promise((resolve) => {
		timeoutId = setTimeout(resolve, timeout);
	});

	return { promise, timeoutId: timeoutId! };
}

export abstract class Consumer<T, TReturn = T> {
	protected _killPacket?: IteratorReturnResult<TReturn | undefined>;
	protected _resolve?: () => void;
	private _backpressure: number;
	private _timeoutId?: NodeJS.Timeout;
	currentNode: ConsumerNode<T, TReturn> | null;

	id: number;
	isAlive: boolean;
	stream: WritableConsumableStream<T, TReturn>;
	timeout?: number;

	constructor(stream: WritableConsumableStream<T, TReturn>, id: number, startNode: ConsumerNode<T, TReturn>, timeout?: number) {
		this.id = id;
		this._backpressure = 0;
		this.currentNode = startNode;
		this.timeout = timeout;
		this.isAlive = true;
		this.stream = stream;
		this.stream.setConsumer(this.id, this);
	}

	applyBackpressure(_: IteratorResult<T, TReturn>): void {
		this._backpressure++;
	}

	clearActiveTimeout(_?: IteratorResult<T, TReturn | undefined>) {
		if (this._timeoutId !== undefined) {
			clearTimeout(this._timeoutId);
			delete this._timeoutId;
		}
	}

	protected destroy(): void {
		this.isAlive = false;
		this.resetBackpressure();
		this.stream.removeConsumer(this.id);
	}

	getBackpressure(): number {
		return this._backpressure;
	}

	getStats(): ConsumerStats {
		const stats: ConsumerStats = {
			backpressure: this._backpressure,
			id: this.id
		};

		if (this.timeout != null) {
			stats.timeout = this.timeout;
		}
		return stats;
	}

	kill(value?: TReturn): void {
		this._killPacket = { done: true, value };
		if (this._timeoutId !== undefined) {
			this.clearActiveTimeout(this._killPacket);
		}
		this._killPacket = { done: true, value };
		this.destroy();

		if (this._resolve) {
			this._resolve();
			delete this._resolve;
		}
	}

	releaseBackpressure(_: IteratorResult<T, TReturn>): void {
		this._backpressure--;
	}

	private resetBackpressure(): void {
		this._backpressure = 0;
	}

	[Symbol.asyncIterator]() {
		return this;
	}

	protected async waitForNextItem(timeout?: number): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			this._resolve = resolve;
			let timeoutId: NodeJS.Timeout;

			if (timeout !== undefined) {
				// Create the error object in the outer scope in order
				// to get the full stack trace.
				const error = new Error('Stream consumer iteration timed out');
				(async () => {
					const delay = wait(timeout);
					timeoutId = delay.timeoutId;
					await delay.promise;
					error.name = 'TimeoutError';
					delete this._resolve;
					reject(error);
				})();
			}

			this._timeoutId = timeoutId!;
		});
	}

	write(packet: IteratorResult<T, TReturn>): void {
		this.clearActiveTimeout(packet);
		this.applyBackpressure(packet);
		if (this._resolve) {
			this._resolve();
			delete this._resolve;
		}
	}
}
