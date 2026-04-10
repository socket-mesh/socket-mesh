import { AuthToken, SignedAuthToken } from '@socket-mesh/auth';
import ws from 'isomorphic-ws';

import { MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap } from './maps/method-map.js';
import { AnyPacket, IncomingPacket } from './packet.js';
import { AnyResponse, OutgoingResponse } from './response.js';

export interface AuthenticatedChangeEvent {
	authToken: AuthToken | null,
	isAuthenticated: true,
	signedAuthToken: SignedAuthToken,
	wasAuthenticated: boolean
}

export interface AuthenticateEvent {
	authToken: AuthToken | null,
	signedAuthToken: SignedAuthToken,
	wasSigned: boolean
}

export type AuthStateChangeEvent = AuthenticatedChangeEvent | DeauthenticatedChangeEvent;

export interface BadAuthTokenEvent {
	error: Error,
	signedAuthToken: SignedAuthToken
}

export interface CloseEvent {
	code: number,
	reason?: string
}

export interface ConnectEvent {
	authError?: Error,
	id: null | string,
	isAuthenticated: boolean,
	pingTimeoutMs: number
}

export type ConnectingEvent = object;

export interface DeauthenticatedChangeEvent {
	isAuthenticated: false,
	wasAuthenticated: true
}

export interface DeauthenticateEvent {
	authToken: AuthToken | null,
	signedAuthToken: SignedAuthToken
}

export interface DisconnectEvent {
	code: number,
	reason?: string
}

export interface ErrorEvent {
	error: Error
}

export interface MessageEvent {
	data: string | ws.RawData,
	isBinary: boolean
}

export type PingEvent = object;

export type PongEvent = object;

export interface RemoveAuthTokenEvent {
	oldAuthToken: SignedAuthToken
}

export interface RequestEvent {
	request: AnyPacket
}

export interface ResponseEvent {
	response: AnyResponse
}

export type SocketEvent =
	AuthenticateEvent
	| AuthStateChangeEvent
	| BadAuthTokenEvent
	| CloseEvent
	| ConnectEvent
	| ConnectingEvent
	| DeauthenticateEvent
	| DisconnectEvent
	| ErrorEvent
	| MessageEvent
	| PingEvent
	| PongEvent
	| RemoveAuthTokenEvent
	| RequestEvent
	| ResponseEvent;

export interface TypedRequestEvent<
	TIncoming extends MethodMap,
	TService extends ServiceMap = {}
> {
	request: IncomingPacket<TIncoming, TService>
}

export interface TypedResponseEvent<
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap = {},
	TService extends ServiceMap = {}
> {
	response: OutgoingResponse<TOutgoing, TPrivateOutgoing, TService>
}

export type TypedSocketEvent<
	TIncoming extends MethodMap,
	TOutgoing extends PublicMethodMap,
	TPrivateOutgoing extends PrivateMethodMap = {},
	TService extends ServiceMap = {}
> =
	AuthenticateEvent
	| AuthStateChangeEvent
	| BadAuthTokenEvent
	| CloseEvent
	| ConnectEvent
	| ConnectingEvent
	| DeauthenticateEvent
	| DisconnectEvent
	| ErrorEvent
	| MessageEvent
	| PingEvent
	| PongEvent
	| RemoveAuthTokenEvent
	| TypedRequestEvent<TIncoming, TService>
	| TypedResponseEvent<TOutgoing, TPrivateOutgoing, TService>;
