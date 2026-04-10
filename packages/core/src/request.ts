import { toArray } from './utils.js';

export type AnyRequest = MethodRequest | ServiceRequest;

export interface InvokeMethodRequest extends TransmitMethodRequest {
	ackTimeoutMs: false | number,
	callback: ((err: Error | null, result?: unknown) => void) | null,
	cid: number,
	timeoutId?: NodeJS.Timeout
}

export interface InvokeServiceRequest extends TransmitServiceRequest {
	ackTimeoutMs: false | number,
	callback: ((err: Error | null, result?: unknown) => void) | null,
	cid: number,
	timeoutId?: NodeJS.Timeout
}

export type MethodRequest = InvokeMethodRequest | TransmitMethodRequest;

export interface Request {
	promise: Promise<void>,
	sentCallback?: (err?: Error) => void
}

export type ServiceRequest = InvokeServiceRequest | TransmitServiceRequest;

export interface TransmitMethodRequest extends Request {
	data?: unknown,
	method: string
}

export interface TransmitServiceRequest extends Request {
	data?: unknown,
	method: string,
	service: string
}

export function abortRequest(request: AnyRequest, err: Error): void {
	if (request.sentCallback) {
		request.sentCallback(err);
	}

	if ('callback' in request && request.callback) {
		request.callback(err);
	}
}

export function isRequestDone(request: AnyRequest): boolean {
	if ('callback' in request) {
		return (request.callback === null);
	}

	return !request.sentCallback;
}

export class RequestCollection {
	private readonly _callbacks: (() => void)[];
	private readonly _requests: AnyRequest[];

	constructor(requests: AnyRequest | AnyRequest[]) {
		this._requests = toArray(requests).filter(req => !isRequestDone(req));
		this._callbacks = [];
	}

	public isDone(): boolean {
		return this._requests.length === 0;
	}

	public get items(): ReadonlyArray<AnyRequest> {
		return this._requests;
	}

	public listen(cb: () => void): void {
		for (const req of this._requests) {
			this._callbacks.push(cb);

			req.promise.finally(() => {
				const i = this._requests.indexOf(req);
				this._requests.splice(i, 1);

				if (!this._requests.length) {
					for (const cb of this._callbacks) {
						cb();
					}
				}
			});
		}
	}

	[Symbol.iterator]() {
		const values = this._requests;
		let index = 0;

		return {
			next() {
				if (index < values.length) {
					const val = values[index];
					index++;
					return { done: false, value: val };
				} else return { done: true };
			}
		};
	}
}
