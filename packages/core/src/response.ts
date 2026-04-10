import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap } from './maps/method-map.js';

export type AnyResponse = DataResponse | ErrorResponse | Response;

export interface DataResponse extends Response {
	data: unknown
}

export interface ErrorResponse extends Response {
	error: Error
}

export interface OutgoingDataResponse<T> extends Response {
	data: T
}

export type OutgoingMethodDataResponse<TMethodMap extends MethodMap> =
	{ [TMethod in keyof TMethodMap]:
		OutgoingDataResponse<ReturnType<TMethodMap[TMethod]>>
	}[keyof TMethodMap];

// Typed response variants for use in subclass typed event APIs.
// These are structurally assignable to AnyResponse so existing
// runtime/transport code that operates on AnyResponse continues to work.

export type OutgoingResponse<
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap,
	TService extends ServiceMap
> =
	ErrorResponse
	| OutgoingMethodDataResponse<TOutgoing>
	| OutgoingMethodDataResponse<TPrivateOutgoing>
	| OutgoingServiceDataResponse<TService>
	| Response;

export type OutgoingServiceDataResponse<TServiceMap extends ServiceMap> =
	{ [TService in keyof TServiceMap]:
		{ [TMethod in keyof TServiceMap[TService]]:
			OutgoingDataResponse<ReturnType<TServiceMap[TService][TMethod]>>
		}[keyof TServiceMap[TService]]
	}[keyof TServiceMap];

export interface Response {
	rid: number,
	timeoutAt?: Date
}

export function isResponsePacket(packet?: unknown): packet is AnyResponse {
	return (
		packet !== null
		&& typeof packet === 'object'
		&& 'rid' in (packet as object)
	);
}
