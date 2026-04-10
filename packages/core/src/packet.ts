import { MethodMap, ServiceMap } from './maps/method-map.js';

export type AnyPacket = MethodPacket | ServicePacket;

export type IncomingMethodPacket<TMethodMap extends MethodMap> =
	{ [TMethod in keyof TMethodMap]:
		IncomingMethodRequestPacket<TMethodMap, TMethod>
	}[keyof TMethodMap];

export interface IncomingMethodRequestPacket<
	TMethodMap extends MethodMap,
	TMethod extends keyof TMethodMap
> extends RequestPacket {
	ackTimeoutMs?: boolean | number,
	data: Parameters<TMethodMap[TMethod]>[0],
	method: TMethod
}

export type IncomingPacket<
	TIncoming extends MethodMap,
	TService extends ServiceMap
> = IncomingMethodPacket<TIncoming> | IncomingServicePacket<TService>;

export type IncomingServicePacket<TServiceMap extends ServiceMap> =
	{ [TService in keyof TServiceMap]:
		{ [TMethod in keyof TServiceMap[TService]]:
			IncomingServiceRequestPacket<TServiceMap, TService, TMethod>
		}[keyof TServiceMap[TService]]
	}[keyof TServiceMap];

export interface IncomingServiceRequestPacket<
	TServiceMap extends ServiceMap,
	TService extends keyof TServiceMap,
	TMethod extends keyof TServiceMap[TService]
> extends RequestPacket {
	ackTimeoutMs?: boolean | number,
	data?: Parameters<TServiceMap[TService][TMethod]>[0],
	method: TMethod,
	service: TService
}

export type MethodPacket = MethodRequestPacket;

// Typed packet variants for use in subclass typed event APIs.
// These are structurally assignable to AnyPacket so existing
// runtime/transport code that operates on AnyPacket continues to work.

export interface MethodRequestPacket extends RequestPacket {
	ackTimeoutMs?: boolean | number,
	data?: unknown,
	method: string
}

interface RequestPacket {
	cid?: number
}

export type ServicePacket = ServiceRequestPacket;

export interface ServiceRequestPacket extends RequestPacket {
	ackTimeoutMs?: boolean | number,
	data?: unknown,
	method: string,
	service: string
}

export function isRequestPacket(packet: unknown): packet is AnyPacket;
export function isRequestPacket<
	TIncoming extends MethodMap,
	TService extends ServiceMap = {}
>(packet: unknown): packet is IncomingPacket<TIncoming, TService>;
export function isRequestPacket(packet: unknown): packet is AnyPacket {
	return (
		packet !== null
		&& typeof packet === 'object'
		&& 'method' in packet
	);
}
