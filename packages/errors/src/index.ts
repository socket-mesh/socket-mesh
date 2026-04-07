import { decycle } from './decycle.js';
export { decycle } from './decycle.js';

export class AbortError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'AbortError';

		Object.setPrototypeOf(this, AbortError.prototype);
	}
}

// For any other auth error; not specifically related to the auth token itself.
export class AuthError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'AuthError';

		Object.setPrototypeOf(this, AuthError.prototype);
	}
}

// For any other auth token error.
export class AuthTokenError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'AuthTokenError';

		Object.setPrototypeOf(this, AuthTokenError.prototype);
	}
}

export class AuthTokenExpiredError extends AuthTokenError {
	expiry: Date;

	constructor(message: string, expiry: Date) {
		super(message);

		this.name = 'AuthTokenExpiredError';
		this.expiry = expiry;

		Object.setPrototypeOf(this, AuthTokenExpiredError.prototype);
	}
}

export class AuthTokenInvalidError extends AuthTokenError {
	constructor(message: string) {
		super(message);

		this.name = 'AuthTokenInvalidError';

		Object.setPrototypeOf(this, AuthTokenInvalidError.prototype);
	}
}

export class AuthTokenNotBeforeError extends AuthTokenError {
	date: Date;

	constructor(message: string, date: Date) {
		super(message);

		this.name = 'AuthTokenNotBeforeError';
		this.date = date;

		Object.setPrototypeOf(this, AuthTokenNotBeforeError.prototype);
	}
}

export class BadConnectionError extends Error {
	type: string;

	constructor(message: string, type: string) {
		super(message);

		this.name = 'BadConnectionError';
		this.type = type;

		Object.setPrototypeOf(this, BadConnectionError.prototype);
	}
}

export class BrokerError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'BrokerError';

		Object.setPrototypeOf(this, BrokerError.prototype);
	}
}

export class HttpServerError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'HttpServerError';

		Object.setPrototypeOf(this, HttpServerError.prototype);
	}
}

export class InvalidActionError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'InvalidActionError';

		Object.setPrototypeOf(this, InvalidActionError.prototype);
	}
}

export class InvalidArgumentsError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'InvalidArgumentsError';

		Object.setPrototypeOf(this, InvalidArgumentsError.prototype);
	}
}

export class InvalidMessageError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'InvalidMessageError';

		Object.setPrototypeOf(this, InvalidMessageError.prototype);
	}
}

export class InvalidOptionsError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'InvalidOptionsError';

		Object.setPrototypeOf(this, InvalidOptionsError.prototype);
	}
}

export class PluginBlockedError extends Error {
	type: string;

	constructor(message: string, type: string) {
		super(message);

		this.name = 'PluginBlockedError';
		this.type = type;

		Object.setPrototypeOf(this, PluginBlockedError.prototype);
	}
}

export class PluginError extends Error {
	type: string;

	constructor(message: string, type: string) {
		super(message);

		this.name = 'PluginError';
		this.type = type;

		Object.setPrototypeOf(this, PluginError.prototype);
	}
}

export class ProcessExitError extends Error {
	code: number;

	constructor(message: string, code: number) {
		super(message);

		this.name = 'ProcessExitError';
		this.code = code;

		Object.setPrototypeOf(this, ProcessExitError.prototype);
	}
}

export class ResourceLimitError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'ResourceLimitError';

		Object.setPrototypeOf(this, ResourceLimitError.prototype);
	}
}

export class ServerProtocolError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'ServerProtocolError';

		Object.setPrototypeOf(this, ServerProtocolError.prototype);
	}
}

export class SilentPluginBlockedError extends Error {
	type: string;

	constructor(message: string, type: string) {
		super(message);

		this.name = 'SilentPluginBlockedError';
		this.type = type;

		Object.setPrototypeOf(this, SilentPluginBlockedError.prototype);
	}
}

export class SocketClosedError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'SocketClosedError';

		Object.setPrototypeOf(this, ServerProtocolError.prototype);
	}
}

export class SocketProtocolError extends Error {
	code: number;

	constructor(message: string, code: number) {
		super(message);

		this.name = 'SocketProtocolError';
		this.code = code;

		Object.setPrototypeOf(this, SocketProtocolError.prototype);
	}
}

export class TimeoutError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'TimeoutError';

		Object.setPrototypeOf(this, TimeoutError.prototype);
	}
}

export class UnknownError extends Error {
	constructor(message: string) {
		super(message);

		this.name = 'UnknownError';

		Object.setPrototypeOf(this, UnknownError.prototype);
	}
}

export const SocketProtocolErrorStatuses: { [code: number]: string } = {
	1001: 'Socket was disconnected',
	1002: 'A WebSocket protocol error was encountered',
	1003: 'Server terminated socket because it received invalid data',
	1005: 'Socket closed without status code',
	1006: 'Socket hung up',
	1007: 'Message format was incorrect',
	1008: 'Encountered a policy violation',
	1009: 'Message was too big to process',
	1010: 'Client ended the connection because the server did not comply with extension requirements',
	1011: 'Server encountered an unexpected fatal condition',
	4000: 'Server ping timed out',
	4001: 'Client pong timed out',
	4002: 'Server failed to sign auth token',
	4003: 'Failed to complete handshake',
	4004: 'Client failed to save auth token',
	4005: 'Did not receive #handshake from client before timeout',
	4006: 'Failed to bind socket to message broker',
	4007: 'Client connection establishment timed out',
	4008: 'Server rejected handshake from client',
	4009: 'Server received a message before the client handshake'
};

export const SocketProtocolIgnoreStatuses: { [code: number]: string } = {
	1000: 'Socket closed normally',
	1001: 'Socket hung up'
};

export type DehydratedError = any;

export interface HandshakeError extends Error {
	statusCode: number
}

// Convert an error into a JSON-compatible type which can later be hydrated
// back to its *original* form.
export function dehydrateError(error: any): DehydratedError {
	let dehydratedError: any;

	if (error && typeof error === 'object') {
		dehydratedError = {
			message: error.message
		};

		for (const i of Object.keys(error)) {
			dehydratedError[i] = error[i];
		}
	} else if (typeof error === 'function') {
		dehydratedError = '[function ' + (typeof error.name === 'string' ? error.name : 'anonymous') + ']';
	} else {
		dehydratedError = error;
	}

	return decycle(dehydratedError);
}

// Convert a dehydrated error back to its *original* form.
export function hydrateError(error: DehydratedError): any {
	let hydratedError: null | { [ key: string ]: any } = null;

	if (error != null) {
		if (typeof error === 'object') {
			hydratedError = new Error(typeof error.message === 'string' ? error.message : 'Invalid error message format');

			if (typeof error.name === 'string') {
				hydratedError.name = error.name;
			}

			for (const i of Object.keys(error)) {
				if (hydratedError[i] === undefined) {
					hydratedError[i as keyof Error] = error[i];
				}
			}
		} else {
			hydratedError = error;
		}
	}

	return hydratedError;
}
