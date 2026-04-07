import { HandshakeOptions, HandshakeStatus } from '@socket-mesh/client';
import { toError, wait } from '@socket-mesh/core';
import { dehydrateError, HandshakeError } from '@socket-mesh/errors';

import { processAuthentication, validateAuthToken } from './authenticate.js';
import { ServerRequestHandlerArgs } from './server-request-handler.js';

const HANDSHAKE_REJECTION_STATUS_CODE = 4008;

export async function handshakeHandler(
	{ options, socket, transport }: ServerRequestHandlerArgs<HandshakeOptions>
): Promise<HandshakeStatus> {
	const server = socket.server;
	const wasAuthenticated = !!transport.signedAuthToken;
	const authInfo = await validateAuthToken(server.auth, options.authToken);

	for (const plugin of server.plugins) {
		if (plugin.onHandshake) {
			try {
				await plugin.onHandshake({
					authInfo: 'authError' in authInfo
						? { authError: authInfo.authError, signedAuthToken: options.authToken }
						: { authToken: authInfo.authToken, signedAuthToken: options.authToken },
					socket,
					transport
				});
			} catch (err) {
				const error = toError(err);

				if (!('statusCode' in error)) {
					(error as HandshakeError).statusCode = HANDSHAKE_REJECTION_STATUS_CODE;
				}
				throw error;
			}
		}
	}

	let authError: Error | undefined = undefined;
	let changed = false;

	try {
		changed = await processAuthentication(socket, transport, authInfo);

		if (socket.status === 'closed') {
			return {
				authToken: options.authToken,
				id: socket.id,
				pingTimeoutMs: server.pingTimeoutMs
			};
		}
	} catch (err) {
		if (options.authToken) {
			// Because the token is optional as part of the handshake, we don't count
			// it as an error if the token wasn't provided.
			authError = dehydrateError(err);
		}
	}

	transport.setReadyStatus(server.pingTimeoutMs, authError);

	// Needs to be executed after the connection event to allow consumers to be setup.
	await wait(0);

	if (changed) {
		transport.triggerAuthenticationEvents(false, wasAuthenticated);
	}

	if (authError) {
		return {
			authError,
			id: socket.id,
			pingTimeoutMs: server.pingTimeoutMs
		};
	}

	return {
		authToken: options.authToken,
		id: socket.id,
		pingTimeoutMs: server.pingTimeoutMs
	};
}
