import { AnyRequest, AnyResponse, Plugin, SendRequestPluginArgs, SendResponsePluginArgs } from '@socket-mesh/core';

export interface BatchingPluginOptions<TType = string> {
	// This lets you specify the size of each batch in milliseconds.
	batchInterval?: number,

	// Whether or not to start batching messages immediately after the connection handshake completes. This is useful for handling
	// connection recovery when the client tries to resubscribe to a large number of channels in a very short amount of time. Defaults to false.
	// This lets you specify how long to enable batching (in milliseconds) following a successful socket handshake.
	batchOnHandshakeDuration?: false | number,

	type: TType
}

export abstract class BatchingPlugin<
	TType extends string
> implements Plugin {
	private _batchingIntervalId: NodeJS.Timeout | null;
	private _handshakeTimeoutId: NodeJS.Timeout | null;

	private _isBatching: boolean;
	public batchInterval: number;
	public batchOnHandshakeDuration: boolean | number;

	type: TType;

	constructor(options: BatchingPluginOptions<TType>) {
		this._isBatching = false;
		this.type = options.type;
		this.batchInterval = options.batchInterval || 50;
		this.batchOnHandshakeDuration = options.batchOnHandshakeDuration ?? false;
		this._batchingIntervalId = null;
		this._handshakeTimeoutId = null;
	}

	public cancelBatching(): void {
		if (this._batchingIntervalId !== null) {
			clearInterval(this._batchingIntervalId);
		}

		this._isBatching = false;
		this._batchingIntervalId = null;
	}

	protected abstract flush(): void;

	public get isBatching(): boolean {
		return this._isBatching || this._batchingIntervalId !== null;
	}

	public onDisconnected(): void {
		this.cancelBatching();
	}

	public onReady(): void {
		if (this._isBatching) {
			this.start();
		} else if (typeof this.batchOnHandshakeDuration === 'number' && this.batchOnHandshakeDuration > 0) {
			this.start();
			this._handshakeTimeoutId = setTimeout(() => {
				this.stop();
			}, this.batchOnHandshakeDuration);
		}
	}

	private start(): void {
		if (this._batchingIntervalId !== null) {
			return;
		}

		this._batchingIntervalId = setInterval(() => {
			this.flush();
		}, this.batchInterval);
	}

	public startBatching(): void {
		this._isBatching = true;
		this.start();
	}

	private stop(): void {
		if (this._batchingIntervalId !== null) {
			clearInterval(this._batchingIntervalId);
		}

		this._batchingIntervalId = null;

		if (this._handshakeTimeoutId !== null) {
			clearTimeout(this._handshakeTimeoutId);
			this._handshakeTimeoutId = null;
		}

		this.flush();
	}

	public stopBatching(): void {
		this._isBatching = false;
		this.stop();
	}
}

export class RequestBatchingPlugin extends BatchingPlugin<'requestBatching'> {
	private _continue: ((requests: AnyRequest[], cb?: (error?: Error) => void) => void) | null;
	private _requests: AnyRequest[];

	constructor(options?: Omit<BatchingPluginOptions, 'type'>) {
		super({
			...(options ?? {}),
			type: 'requestBatching'
		});

		this._requests = [];
		this._continue = null;
	}

	public override cancelBatching(): void {
		super.cancelBatching();

		this._requests = [];
		this._continue = null;
	}

	protected override flush() {
		if (this._requests.length) {
			if (this._continue) {
				this._continue(this._requests);
				this._continue = null;
			}

			this._requests = [];
		}
	}

	public sendRequest({ cont, requests }: SendRequestPluginArgs): void {
		if (!this.isBatching) {
			cont(requests);
			return;
		}

		this._continue = cont;
		this._requests.push(...requests);
	}
}

export class ResponseBatchingPlugin extends BatchingPlugin<'responseBatching'> {
	private _continue: ((requests: AnyResponse[], cb?: (error?: Error) => void) => void) | null;
	private _responses: AnyResponse[];

	constructor(options?: Omit<BatchingPluginOptions, 'type'>) {
		super({
			...(options ?? {}),
			type: 'responseBatching'
		});

		this._responses = [];
		this._continue = null;
	}

	protected override flush() {
		if (this._responses.length) {
			if (this._continue) {
				this._continue(this._responses);
				this._continue = null;
			}

			this._responses = [];
		}
	}

	public sendResponse({ cont, responses }: SendResponsePluginArgs): void {
		if (!this.isBatching) {
			cont(responses);
			return;
		}

		this._continue = cont;
		this._responses.push(...responses);
	}
}
