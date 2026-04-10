import { ClientSocket, ClientSocketOptions, LocalStorageAuthEngine, OfflinePlugin } from '@socket-mesh/client';
import { AuthStateChangeEvent, CloseEvent, DisconnectEvent, RequestHandlerArgs, SocketStatus, toError, wait } from '@socket-mesh/core';
import localStorage from '@socket-mesh/local-storage';
import { listen, Server, ServerRequestHandlerArgs } from '@socket-mesh/server';
import jwt from 'jsonwebtoken';
import assert from 'node:assert';
import { afterEach, beforeEach, describe, it, mock } from 'node:test';

// Add to the global scope like in browser.
global.localStorage = localStorage;

const PORT_NUMBER = 8009;
const TOKEN_EXPIRY_IN_SECONDS = 60 * 60 * 24 * 366 * 5000;
const AUTH_TOKEN_NAME = 'socketmesh.authToken';

const VALID_SIGNED_AUTH_TOKEN_BOB = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImJvYiIsImV4cCI6MzE2Mzc1ODk3ODIxNTQ4NywiaWF0IjoxNTAyNzQ3NzQ2fQ.GLf_jqi_qUSCRahxe2D2I9kD8iVIs0d4xTbiZMRiQq4';
const VALID_SIGNED_AUTH_TOKEN_KATE = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImthdGUiLCJleHAiOjMxNjM3NTg5NzgyMTU0ODcsImlhdCI6MTUwMjc0Nzc5NX0.Yfb63XvDt9Wk0wHSDJ3t7Qb1F0oUVUaM5_JKxIE2kyw';
const INVALID_SIGNED_AUTH_TOKEN = 'fakebGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.fakec2VybmFtZSI6ImJvYiIsImlhdCI6MTUwMjYyNTIxMywiZXhwIjoxNTAyNzExNjEzfQ.fakemYcOOjM9bzmS4UYRvlWSk_lm3WGHvclmFjLbyOk';
const SERVER_AUTH_KEY = 'testkey';

interface LoginRequest {
	username: string
}

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
type MyChannels = {
	bar: string | { def: number },
	foo: string | { abc: number },
	priv: string,
	priv2: string
};

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
type ServerIncomingMap = {
	login: (req: LoginRequest) => void,
	performTask: (num: number) => void,
	setAuthKey: (secret: jwt.Secret) => void
};

const allowedUsers: { [name: string]: true } = {
	alice: true,
	bob: true,
	kate: true
};

let client: ClientSocket<ServerIncomingMap, MyChannels>;
let server: Server<ServerIncomingMap, MyChannels>;

let wasPerformTaskTriggered: boolean;

async function loginHandler({ options, transport }: ServerRequestHandlerArgs<LoginRequest>): Promise<void> {
	if (!allowedUsers[options.username]) {
		const err = new Error('Failed to login');
		err.name = 'FailedLoginError';
		throw err;
	}

	const authToken = {
		exp: Math.round(Date.now() / 1000) + TOKEN_EXPIRY_IN_SECONDS,
		username: options.username
	};

	transport.setAuthorization(authToken);
}

async function performTaskHandler({ options }: RequestHandlerArgs<number>): Promise<void> {
	wasPerformTaskTriggered = true;
	await wait(options);
}

async function setAuthKeyHandler({ options: secret, socket }: ServerRequestHandlerArgs<jwt.Secret>): Promise<void> {
	socket.server!.auth.authKey = secret;
}

const clientOptions: ClientSocketOptions<ServerIncomingMap> = {
	ackTimeoutMs: 200,
	address: `ws://127.0.0.1:${PORT_NUMBER}`,
	authEngine: { authTokenName: AUTH_TOKEN_NAME }
};

describe('Client Tests', function () {
	beforeEach(async function () {
		server = listen<ServerIncomingMap, MyChannels>(
			PORT_NUMBER,
			{
				ackTimeoutMs: 200,
				authEngine: { authKey: SERVER_AUTH_KEY },
				handlers: {
					login: loginHandler,
					performTask: performTaskHandler,
					setAuthKey: setAuthKeyHandler
				}
			}
		);

		wasPerformTaskTriggered = false;
		await server.listen('ready').once(100);
	});

	afterEach(async function () {
		const cleanupTasks: Promise<DisconnectEvent | void>[] = [];

		global.localStorage.removeItem(AUTH_TOKEN_NAME);

		// eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
		if (client) {
			if (client.status !== 'closed') {
				cleanupTasks.push(
					Promise.race([
						client.listen('disconnect').once(),
						client.listen('connectAbort').once()
					])
				);
			}

			client.disconnect();
		}

		cleanupTasks.push(
			(async () => {
				server.httpServer.close();
				await server.close();
			})()
		);

		await Promise.all(cleanupTasks);
	});

	describe('Creation', function () {
		it('Should automatically connect socket on creation by default', async function () {
			client = new ClientSocket(clientOptions);

			assert.strictEqual<SocketStatus>(client.status, 'connecting');
		});

		it('Should not automatically connect socket if autoConnect is set to false', async function () {
			client = new ClientSocket({
				autoConnect: false,
				...clientOptions
			});

			assert.strictEqual<SocketStatus>(client.status, 'closed');
		});
	});

	describe('Errors', function () {
		it('Should be able to emit the error event locally on the socket', (context, done) => {
			client = new ClientSocket(clientOptions);

			(async () => {
				for await (const { error } of client.listen('error')) {
					try {
						assert.notEqual(error, null);
						assert.strictEqual(error.name, 'CustomError');
					} finally {
						done();
					}
				}
			})();

			(async () => {
				for await (const _ of client.listen('connect')) {
					const error = new Error('Custom error');
					error.name = 'CustomError';
					client.emit('error', { error });
				}
			})();
		});
	});

	describe('Authentication', function () {
		it('Should not send back error if JWT is not provided in handshake', async function () {
			client = new ClientSocket(clientOptions);

			const event = await client.listen('connect').once(100);

			assert.strictEqual(event.authError, undefined);
		});

		it('Should be authenticated on connect if previous JWT token is present', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);

			const event = await client.listen('connect').once(100);

			assert.strictEqual(client.signedAuthToken, VALID_SIGNED_AUTH_TOKEN_BOB);
			assert.strictEqual(client.authToken?.username, 'bob');
			assert.strictEqual(event.authError, undefined);
		});

		it('Should send back error if JWT is invalid during handshake', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);

			let event = await client.listen('connect').once(100);
			assert.notEqual(event, null);
			assert.strictEqual(event.isAuthenticated, true);
			assert.strictEqual(event.authError, undefined);

			assert.notEqual(client.signedAuthToken, null);
			assert.notEqual(client.authToken, null);

			// Change the setAuthKey to invalidate the current token.
			await client.invoke('setAuthKey', 'differentAuthKey');

			client.disconnect();
			client.connect();

			event = await client.listen('connect').once(100);

			assert.strictEqual(event.isAuthenticated, false);
			assert.notEqual(event.authError, null);
			assert.strictEqual(event.authError!.name, 'AuthTokenInvalidError');

			// When authentication fails, the auth token properties on the client
			// socket should be set to null; that way it's not going to keep
			// throwing the same error every time the socket tries to connect.
			assert.strictEqual(client.signedAuthToken, null);
			assert.strictEqual(client.authToken, null);

			// Set authKey back to what it was.
			await client.invoke('setAuthKey', SERVER_AUTH_KEY);
		});

		it('Should allow switching between users', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);
			let wasAuthenticateTriggered = false;
			let wasAuthStateChangeTriggered = false;

			await client.listen('connect').once(100);

			assert.strictEqual(client.signedAuthToken, VALID_SIGNED_AUTH_TOKEN_BOB);
			assert.strictEqual(client.authToken?.username, 'bob');

			(async () => {
				await client.listen('authenticate').once();
				wasAuthenticateTriggered = true;
				assert.notEqual(client.authToken, null);
				assert.strictEqual(client.authToken?.username, 'alice');
			})();

			(async () => {
				await client.listen('authStateChange').once();
				wasAuthStateChangeTriggered = true;
			})();

			await client.invoke('login', { username: 'alice' });

			await wait(100);
			assert.strictEqual(wasAuthenticateTriggered, true);
			assert.strictEqual(wasAuthStateChangeTriggered, true);
		});

		it('If token engine signing is synchronous, authentication can be captured using the authenticate event', async function () {
			// authSignAsync: false
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once(100);

			await Promise.all([
				client.invoke('login', { username: 'bob' }),
				client.listen('authenticate').once(100)
			]);

			// assert.strictEqual(client.authState, AuthState.AUTHENTICATED);
			assert.notEqual(client.authToken, null);
			assert.strictEqual(client.authToken?.username, 'bob');
		});

		it('If token engine signing is asynchronous, authentication can be captured using the authenticate event', async function () {
			// authSignAsync: true
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once(100);

			await Promise.all([
				client.invoke('login', { username: 'bob' }),
				client.listen('authenticate').once(100)
			]);

			// assert.strictEqual(client.authState, AuthState.AUTHENTICATED);
			assert.notEqual(client.authToken, null);
			assert.strictEqual(client.authToken?.username, 'bob');
		});

		it('If token verification is synchronous, authentication can be captured using the authenticate event', async function () {
			client = new ClientSocket(clientOptions);
			// authVerifyAsync: false

			await client.listen('connect').once();

			await Promise.all([
				(async () => {
					await Promise.all([
						client.invoke('login', { username: 'bob' }),
						client.listen('authenticate').once()
					]);
					client.disconnect();
				})(),
				(async () => {
					await client.listen('authenticate').once();
					await client.listen('disconnect').once();

					client.connect();

					const event = await client.listen('connect').once();

					assert.strictEqual(event.isAuthenticated, true);
					assert.notEqual(client.authToken, null);
					assert.strictEqual(client.authToken?.username, 'bob');
				})()
			]);
		});

		it('Should start out in pending authState and switch to unauthenticated if no token exists', async function () {
			client = new ClientSocket(clientOptions);

			// assert.strictEqual(client.authState, AuthState.UNAUTHENTICATED);
			assert.equal(client.signedAuthToken, null);

			(async () => {
				await client.listen('authStateChange').once();

				throw new Error('authState should not change after connecting without a token');
			})();

			await wait(1000);
		});

		it('Should deal with auth engine errors related to saveToken function', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);

			const authEngine = new LocalStorageAuthEngine();

			mock.method(
				authEngine,
				'saveToken',
				() => {
					const err = new Error('Failed to save token');
					err.name = 'FailedToSaveTokenError';
					return Promise.reject(err);
				}
			);

			client = new ClientSocket({
				...clientOptions,
				authEngine
			});

			let caughtError: Error;

			(async () => {
				for await (const { error } of client.listen('error')) {
					caughtError = error;
				}
			})();

			await client.listen('connect').once();
			assert.notEqual(client.authToken, null);
			assert.strictEqual(client.authToken?.username, 'bob');

			await client.authenticate(VALID_SIGNED_AUTH_TOKEN_KATE);

			// The error here comes from the client auth engine and does not prevent the
			// authentication from taking place, it only prevents the token from being
			// stored correctly on the client.
			assert.notEqual(client.authToken, null);
			assert.strictEqual(client.authToken.username, 'kate');
			assert.notEqual(caughtError!, null);
			assert.strictEqual(caughtError!.name, 'FailedToSaveTokenError');
		});

		it('Should gracefully handle authenticate abortion due to disconnection', async function () {
			client = new ClientSocket(clientOptions);
			await client.listen('connect').once();

			const authenticatePromise = client.authenticate(VALID_SIGNED_AUTH_TOKEN_BOB);
			client.disconnect();

			try {
				await authenticatePromise;
			} catch (err) {
				assert.notEqual(err, null);
				assert.strictEqual(toError(err).name, 'BadConnectionError');
				assert.equal(client.signedAuthToken, null);
			}
		});

		it('Should go through the correct sequence of authentication state changes when dealing with disconnections; part 1', async function () {
			client = new ClientSocket(clientOptions);

			const expectedAuthStateChanges = [
				'false->true'
			];
			const authStateChanges: string[] = [];

			(async () => {
				for await (const state of client.listen('authStateChange')) {
					authStateChanges.push(`${state.wasAuthenticated}->${state.isAuthenticated}`);
				}
			})();

			assert.equal(client.signedAuthToken, null);

			await client.listen('connect').once();

			assert.equal(client.signedAuthToken, null);

			(async () => {
				await Promise.all([
					client.invoke('login', { username: 'bob' }),
					client.listen('authenticate').once()
				]);
				client.disconnect();
			})();

			assert.equal(client.signedAuthToken, null);

			const { authToken, signedAuthToken } = await client.listen('authenticate').once();

			assert.notEqual(signedAuthToken, null);
			assert.notEqual(authToken, null);

			assert.notEqual(client.signedAuthToken, null);

			await client.listen('disconnect').once();

			// In case of disconnection, the socket maintains the last known auth state.
			assert.notEqual(client.signedAuthToken, null);

			await client.authenticate(signedAuthToken);

			assert.notEqual(client.signedAuthToken, null);
			assert.strictEqual(JSON.stringify(authStateChanges), JSON.stringify(expectedAuthStateChanges));
			client.closeListeners('authStateChange');
		});

		it('Should go through the correct sequence of authentication state changes when dealing with disconnections; part 2', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);

			const expectedAuthStateChanges = [
				'false->true',
				'true->false',
				'false->true',
				'true->false'
			];
			const authStateChanges: string[] = [];

			(async () => {
				for await (const status of client.listen('authStateChange')) {
					authStateChanges.push(`${status.wasAuthenticated}->${status.isAuthenticated}`);
				}
			})();

			assert.equal(client.signedAuthToken, null);

			await client.listen('connect').once();

			assert.notEqual(client.signedAuthToken, null);

			await client.deauthenticate();
			assert.equal(client.signedAuthToken, null);

			const authenticatePromise = client.authenticate(VALID_SIGNED_AUTH_TOKEN_BOB);
			assert.equal(client.signedAuthToken, null);

			await authenticatePromise;

			assert.notEqual(client.signedAuthToken, null);

			client.disconnect();

			assert.notEqual(client.signedAuthToken, null);
			await client.deauthenticate();
			assert.equal(client.signedAuthToken, null);

			assert.strictEqual(JSON.stringify(authStateChanges), JSON.stringify(expectedAuthStateChanges));
		});

		it('Should go through the correct sequence of authentication state changes when dealing with disconnections; part 3', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);

			const expectedAuthStateChanges = [
				'false->true',
				'true->false'
			];
			const authStateChanges: string[] = [];

			(async () => {
				for await (const status of client.listen('authStateChange')) {
					authStateChanges.push(`${status.wasAuthenticated}->${status.isAuthenticated}`);
				}
			})();

			assert.equal(client.signedAuthToken, null);

			await client.listen('connect').once();

			assert.notEqual(client.signedAuthToken, null);
			const authenticatePromise = client.authenticate(INVALID_SIGNED_AUTH_TOKEN);
			assert.notEqual(client.signedAuthToken, null);

			try {
				await authenticatePromise;
			} catch (err) {
				assert.notEqual(err, null);
				assert.strictEqual(toError(err).name, 'AuthTokenInvalidError');
				assert.equal(client.signedAuthToken, null);
				assert.strictEqual(JSON.stringify(authStateChanges), JSON.stringify(expectedAuthStateChanges));
			}
		});

		it('Should go through the correct sequence of authentication state changes when authenticating as a user while already authenticated as another user', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket(clientOptions);

			const expectedAuthStateChanges = [
				'false->true', 'true->true'
			];
			const authStateChanges: string[] = [];

			(async () => {
				for await (const status of client.listen('authStateChange')) {
					authStateChanges.push(`${status.wasAuthenticated}->${status.isAuthenticated}`);
				}
			})();

			const expectedAuthTokenChanges = [
				VALID_SIGNED_AUTH_TOKEN_BOB,
				VALID_SIGNED_AUTH_TOKEN_KATE
			];
			const authTokenChanges: (null | string)[] = [];

			(async () => {
				for await (const _ of client.listen('authenticate')) {
					authTokenChanges.push(client.signedAuthToken);
				}
			})();

			(async () => {
				for await (const _ of client.listen('deauthenticate')) {
					authTokenChanges.push(client.signedAuthToken);
				}
			})();

			assert.equal(client.signedAuthToken, null);

			await client.listen('connect').once();

			assert.notEqual(client.signedAuthToken, null);
			assert.strictEqual(client.authToken?.username, 'bob');
			const authenticatePromise = client.authenticate(VALID_SIGNED_AUTH_TOKEN_KATE);

			assert.notEqual(client.signedAuthToken, null);

			await authenticatePromise;

			assert.notEqual(client.signedAuthToken, null);
			assert.strictEqual(client.authToken.username, 'kate');
			assert.strictEqual(JSON.stringify(authStateChanges), JSON.stringify(expectedAuthStateChanges));
			assert.strictEqual(JSON.stringify(authTokenChanges), JSON.stringify(expectedAuthTokenChanges));
		});

		it('Should wait for socket to be authenticated before subscribing to waitForAuth channel', async function () {
			client = new ClientSocket(clientOptions);

			const privateChannel = client.channels.subscribe('priv', { waitForAuth: true });
			assert.strictEqual(privateChannel.state, 'pending');

			await client.listen('connect').once(100);
			assert.strictEqual(privateChannel.state, 'pending');

			let isAuthenticated = false;

			(async () => {
				await client.invoke('login', { username: 'bob' });

				isAuthenticated = !!client.signedAuthToken;
			})();

			await client.channels.listen('subscribe').once(100);
			assert.strictEqual(privateChannel.state, 'subscribed');

			client.disconnect();
			assert.strictEqual(privateChannel.state, 'pending');

			client.authenticate(VALID_SIGNED_AUTH_TOKEN_BOB);
			await client.channels.listen('subscribe').once(100);
			assert.strictEqual(privateChannel.state, 'subscribed');

			assert.strictEqual(isAuthenticated, true);
		});

		it('Subscriptions (including those with waitForAuth option) should have priority over the authenticate action', async function () {
			global.localStorage.setItem(AUTH_TOKEN_NAME, VALID_SIGNED_AUTH_TOKEN_BOB);
			client = new ClientSocket({
				...clientOptions,
				plugins: [new OfflinePlugin()]
			});

			const expectedAuthStateChanges = [
				'false->true',
				'true->false'
			];
			let initialSignedAuthToken: null | string;
			const authStateChanges: string[] = [];

			(async () => {
				for await (const status of client.listen('authStateChange')) {
					authStateChanges.push(`${status.wasAuthenticated}->${status.isAuthenticated}`);
				}
			})();

			(async () => {
				let error: Error | null = null;
				try {
					await client.authenticate(INVALID_SIGNED_AUTH_TOKEN);
				} catch (err) {
					error = toError(err);
				}
				assert.notEqual(error, null);
				assert.strictEqual(error!.name, 'AuthTokenInvalidError');
			})();

			const privateChannel = client.channels.subscribe('priv', { waitForAuth: true });
			assert.strictEqual(privateChannel.state, 'pending');

			(async () => {
				const event = await client.listen('connect').once();
				initialSignedAuthToken = client.signedAuthToken;
				assert.strictEqual(event.isAuthenticated, true);
				assert.strictEqual(privateChannel.state, 'pending');

				await Promise.race([
					(async () => {
						const fail = await privateChannel.listen('subscribeFail').once();
						// This shouldn't happen because the subscription should be
						// processed before the authenticate() call with the invalid token fails.
						throw new Error('Failed to subscribe to channel: ' + fail.error.message);
					})(),
					(async () => {
						await privateChannel.listen('subscribe').once();
						assert.strictEqual(privateChannel.state, 'subscribed');
					})()
				]);
			})();

			(async () => {
				// The subscription already went through so it should still be subscribed.
				const { authToken, signedAuthToken } = await client.listen('deauthenticate').once();
				// The subscription already went through so it should still be subscribed.
				assert.strictEqual(privateChannel.state, 'subscribed');
				assert.strictEqual(!!client.signedAuthToken, false);
				assert.strictEqual(client.authToken, null);

				assert.notEqual(authToken, null);
				assert.strictEqual(authToken!.username, 'bob');
				assert.strictEqual(signedAuthToken, initialSignedAuthToken!);

				const privateChannel2 = client.channels.subscribe('priv2', { waitForAuth: true });

				await privateChannel2.listen('subscribe').once();

				// This line should not execute.
				throw new Error('Should not subscribe because the socket is not authenticated');
			})();

			await wait(1000);
			client.closeListeners('authStateChange');
			assert.strictEqual(JSON.stringify(authStateChanges), JSON.stringify(expectedAuthStateChanges));
		});

		it('Should trigger the close event if the socket disconnects in the middle of the handshake phase', async function () {
			client = new ClientSocket(clientOptions);
			let wasAborted = false;
			let wasDiconnected = false;
			let wasClosed = false;

			(async () => {
				await client.listen('connectAbort').once();
				wasAborted = true;
			})();

			(async () => {
				await client.listen('disconnect').once();
				wasDiconnected = true;
			})();

			(async () => {
				await client.listen('close').once();
				wasClosed = true;
			})();

			client.disconnect();

			await wait(0);

			assert.strictEqual(wasAborted, true);
			assert.strictEqual(wasDiconnected, false);
			assert.strictEqual(wasClosed, true);
		});

		it('Should trigger the close event if the socket disconnects after the handshake phase', async function () {
			client = new ClientSocket(clientOptions);
			let wasAborted = false;
			let wasDiconnected = false;
			let wasClosed = false;

			(async () => {
				await client.listen('connectAbort').once();
				wasAborted = true;
			})();

			(async () => {
				await client.listen('disconnect').once();
				wasDiconnected = true;
			})();

			(async () => {
				await client.listen('close').once();
				wasClosed = true;
			})();

			(async () => {
				for await (const _ of client.listen('connect')) {
					client.disconnect();
				}
			})();

			await wait(50);

			assert.strictEqual(wasAborted, false);
			assert.strictEqual(wasDiconnected, true);
			assert.strictEqual(wasClosed, true);
		});
	});

	describe('Transmitting remote events', function () {
		it('Should not throw error on socket if ackTimeout elapses before response to event is sent back', async function () {
			client = new ClientSocket(clientOptions);

			let caughtError: Error | null = null;
			let clientError: Error | null = null;

			(async () => {
				for await (const { error } of client.listen('error')) {
					clientError = error;
				}
			})();

			let responseError: Error | null = null;

			for await (const _ of client.listen('connect')) {
				try {
					await client.invoke('performTask', 1000);
				} catch (err) {
					responseError = toError(err);
				}
				await wait(250);
				try {
					client.disconnect();
				} catch (err) {
					caughtError = toError(err);
				}
				break;
			}

			assert.notEqual(responseError, null);
			assert.strictEqual(clientError, null);
			assert.strictEqual(caughtError, null);
		});
	});

	describe('Pub/sub', function () {
		let publisherClient: ClientSocket<ServerIncomingMap, MyChannels>;
		const lastServerMessage: null | string = null;

		beforeEach(async function () {
			publisherClient = new ClientSocket(clientOptions);

			// server.removePlugin(PluginType.MIDDLEWARE_INBOUND);
			//  server.setPlugin(PluginType.MIDDLEWARE_INBOUND, async (pluginStream) => {
			//   for await (let action of pluginStream) {
			//    if (action.type === 'publishIn') {
			//     lastServerMessage = (action as ActionPublishIn).data;
			//    }
			//   action.allow();
			//  }
			// });
		});

		afterEach(async function () {
			publisherClient.disconnect();
		});

		it('Should receive transmitted publish messages if subscribed to channel', async function () {
			client = new ClientSocket(clientOptions);

			const channel = client.channels.subscribe('foo');
			await channel.listen('subscribe').once();

			(async () => {
				await wait(10);
				publisherClient.channels.transmitPublish('foo', 'hello');
				await wait(20);
				publisherClient.channels.transmitPublish('foo', 'world');
				publisherClient.channels.transmitPublish('foo', { abc: 123 });
				await wait(10);
				channel.close();
			})();

			const receivedMessages: (string | { abc: number })[] = [];

			for await (const message of channel) {
				receivedMessages.push(message);
			}

			assert.strictEqual(receivedMessages.length, 3);
			assert.strictEqual(receivedMessages[0], 'hello');
			assert.strictEqual(receivedMessages[1], 'world');
			assert.strictEqual(JSON.stringify(receivedMessages[2]), JSON.stringify({ abc: 123 }));
		});

		it('Should receive invoked publish messages if subscribed to channel', async function () {
			client = new ClientSocket(clientOptions);

			const channel = client.channels.subscribe('bar');
			await channel.listen('subscribe').once();

			(async () => {
				await wait(10);
				await publisherClient.channels.transmitPublish('bar', 'hi');
				// assert.strictEqual(lastServerMessage, 'hi');
				await wait(20);
				await publisherClient.channels.transmitPublish('bar', 'world');
				// assert.strictEqual(lastServerMessage, 'world');
				await publisherClient.channels.transmitPublish('bar', { def: 123 });
				// assert.strictEqual(JSON.stringify(clientReceivedMessages[2]), JSON.stringify({def: 123}));
				await wait(10);
				channel.close();
			})();

			const clientReceivedMessages: (string | { def: number })[] = [];

			for await (const message of channel) {
				clientReceivedMessages.push(message);
			}

			assert.strictEqual(clientReceivedMessages.length, 3);
			assert.strictEqual(clientReceivedMessages[0], 'hi');
			assert.strictEqual(clientReceivedMessages[1], 'world');
			assert.strictEqual(JSON.stringify(clientReceivedMessages[2]), JSON.stringify({ def: 123 }));
		});
	});

	describe('Reconnecting socket', function () {
		it('Should disconnect socket with code 1000 and reconnect', async function () {
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once();

			let disconnectCode;
			let disconnectReason;

			(async () => {
				for await (const event of client.listen('disconnect')) {
					disconnectCode = event.code;
					disconnectReason = event.reason;
				}
			})();

			client.reconnect();
			await client.listen('connect').once();

			assert.strictEqual(disconnectCode, 1000);
			assert.strictEqual(disconnectReason, undefined);
		});

		it('Should disconnect socket with custom code and data when socket.reconnect() is called with arguments', async function () {
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once();

			let disconnectCode;
			let disconnectReason;

			(async () => {
				const event = await client.listen('disconnect').once();
				disconnectCode = event.code;
				disconnectReason = event.reason;
			})();

			client.reconnect(1000, 'About to reconnect');
			await client.listen('connect').once();

			assert.strictEqual(disconnectCode, 1000);
			assert.strictEqual(disconnectReason, 'About to reconnect');
		});
	});

	describe('Connecting an already connected socket', function () {
		it('Should not disconnect socket if no options are provided', async function () {
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once();

			let disconnectCode: number;
			let disconnectReason: string | undefined;

			(async () => {
				for await (const event of client.listen('disconnect')) {
					disconnectCode = event.code;
					disconnectReason = event.reason;
				}
			})();

			client.connect();

			assert.equal(disconnectCode!, null);
			assert.equal(disconnectReason, null);
		});

		it('Should disconnect socket with code 1000 and connect again if new options are provided', async function () {
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once();

			let disconnectCode: number;
			let disconnectReason: string | undefined;

			(async () => {
				for await (const event of client.listen('disconnect')) {
					disconnectCode = event.code;
					disconnectReason = event.reason;
				}
			})();

			client.connect(clientOptions);
			await client.listen('connect').once();

			assert.equal(disconnectCode!, 1000);
			assert.equal(disconnectReason, 'Socket was disconnected by the client to initiate a new connection');
		});
	});

	describe('Events', function () {
		it('Should trigger unsubscribe event on channel before disconnect event', async function () {
			client = new ClientSocket(clientOptions);
			let hasUnsubscribed = false;

			const fooChannel = client.channels.subscribe('foo');

			(async () => {
				for await (const _ of fooChannel.listen('subscribe')) {
					await wait(100);
					client.disconnect();
				}
			})();

			(async () => {
				for await (const _ of fooChannel.listen('unsubscribe')) {
					hasUnsubscribed = true;
				}
			})();

			await client.listen('disconnect').once();
			assert.strictEqual(hasUnsubscribed, true);
		});

		it('Should not invoke subscribeFail event if connection is aborted', async function () {
			client = new ClientSocket(clientOptions);
			let hasSubscribeFailed = false;
			let hasBadConnectionError = false;
			let wasConnected = false;

			(async () => {
				for await (const _ of client.listen('connect')) {
					wasConnected = true;
					(async () => {
						try {
							await client.invoke('performTask', 123);
						} catch (err) {
							if (toError(err).name === 'BadConnectionError') {
								hasBadConnectionError = true;
							}
						}
					})();

					const fooChannel = client.channels.subscribe('foo');
					(async () => {
						for await (const _ of fooChannel.listen('subscribeFail')) {
							hasSubscribeFailed = true;
						}
					})();

					(async () => {
						await wait(0);
						client.disconnect();
					})();
				}
			})();

			await client.listen('close').once();
			await wait(100);
			assert.strictEqual(wasConnected, true);
			assert.strictEqual(hasBadConnectionError, true);
			assert.strictEqual(hasSubscribeFailed, false);
		});

		it('Should resolve invoke Promise with BadConnectionError before triggering the disconnect event', async function () {
			client = new ClientSocket({
				...clientOptions,
				ackTimeoutMs: 2000,
				plugins: [new OfflinePlugin()]
			});

			const messageList: any[] = [];
			let clientStatus = client.status;

			(async () => {
				for await (const event of client.listen('disconnect')) {
					messageList.push({
						code: event.code,
						reason: event.reason,
						type: 'disconnect'
					});
				}

				for await (const event of client.listen('error')) {
					messageList.push({
						error: event.error,
						type: 'error'
					});
				}
			})();

			(async () => {
				try {
					await client.invoke('performTask', 10);
				} catch (err) {
					clientStatus = client.status;
					messageList.push({
						error: err,
						type: 'error'
					});
				}
			})();
			await client.listen('connect').once();
			client.disconnect();
			await wait(200);
			assert.strictEqual(messageList.length, 2);
			assert.strictEqual(clientStatus, 'closed');
			assert.strictEqual(messageList[0].error.name, 'BadConnectionError');
			assert.strictEqual(messageList[0].type, 'error');
			assert.strictEqual(messageList[1].type, 'disconnect');
		});

		it('Should reconnect if transmit is called on a disconnected socket', async function () {
			client = new ClientSocket(clientOptions);

			let clientError: Error | null = null;

			(async () => {
				for await (const { error } of client.listen('error')) {
					clientError = error;
				}
			})();

			const eventList: string[] = [];

			(async () => {
				for await (const _ of client.listen('connecting')) {
					eventList.push('connecting');
				}
			})();

			(async () => {
				for await (const _ of client.listen('connect')) {
					eventList.push('connect');
				}
			})();

			(async () => {
				for await (const _ of client.listen('disconnect')) {
					eventList.push('disconnect');
				}
			})();

			(async () => {
				for await (const _ of client.listen('close')) {
					eventList.push('close');
				}
			})();

			(async () => {
				for await (const _ of client.listen('connectAbort')) {
					eventList.push('connectAbort');
				}
			})();

			(async () => {
				await client.listen('connect').once();
				client.disconnect();
				client.transmit('performTask', 123);
			})();

			await wait(100);

			const expectedEventList = ['connect', 'close', 'disconnect', 'connecting', 'connect'];

			assert.strictEqual(clientError, null);
			assert.strictEqual(JSON.stringify(eventList), JSON.stringify(expectedEventList));
			assert.strictEqual(wasPerformTaskTriggered, true);
		});

		it('Should correctly handle multiple successive connect and disconnect calls', async function () {
			client = new ClientSocket(clientOptions);

			const eventList: any[] = [];

			let clientError: Error;

			(async () => {
				for await (const { error } of client.listen('error')) {
					clientError = error;
				}
			})();

			(async () => {
				for await (const _ of client.listen('connecting')) {
					eventList.push({
						event: 'connecting'
					});
				}
			})();

			(async () => {
				for await (const _ of client.listen('connect')) {
					eventList.push({
						event: 'connect'
					});
				}
			})();

			(async () => {
				for await (const event of client.listen('connectAbort')) {
					eventList.push({
						code: event.code,
						event: 'connectAbort',
						reason: event.reason
					});
				}
			})();

			(async () => {
				for await (const event of client.listen('disconnect')) {
					eventList.push({
						code: event.code,
						event: 'disconnect',
						reason: event.reason
					});
				}
			})();

			(async () => {
				for await (const event of client.listen('close')) {
					eventList.push({
						code: event.code,
						event: 'close',
						reason: event.reason
					});
				}
			})();

			const onceDisconnect = client.listen('close').once();
			client.disconnect(1000, 'One');
			await onceDisconnect;

			client.listen('connect').once();
			client.connect();
			client.disconnect(4444, 'Two');

			const onceConnect = client.listen('connect').once();

			client.connect();
			await onceConnect;

			client.disconnect(4455, 'Three');
			await wait(100);

			const expectedEventList = [
				{
					code: 1000,
					event: 'connectAbort',
					reason: 'One'
				},
				{
					code: 1000,
					event: 'close',
					reason: 'One'
				},
				{
					event: 'connecting'
				},
				{
					code: 4444,
					event: 'close',
					reason: 'Two'
				},
				{
					code: 4444,
					event: 'connectAbort',
					reason: 'Two'
				},
				{
					event: 'connecting'
				},
				{
					event: 'connect'
				},
				{
					code: 4455,
					event: 'close',
					reason: 'Three'
				},
				{
					code: 4455,
					event: 'disconnect',
					reason: 'Three'
				}
			];
			assert.strictEqual(JSON.stringify(eventList), JSON.stringify(expectedEventList));
		});

		it('Should support event listener timeout using once(timeout) method', async function () {
			client = new ClientSocket(clientOptions);

			let event: AuthStateChangeEvent | null = null;
			let error: Error | null = null;

			try {
				// Since the authStateChange event will not trigger, this should timeout.
				event = await client.listen('authStateChange').once(100);
			} catch (err) {
				error = toError(err);
			}

			assert.strictEqual(event, null);
			assert.notEqual(error, null);
			assert.strictEqual(error!.name, 'TimeoutError');
		});
	});

	describe('Ping/pong', function () {
		it('Should close if ping is not received before timeout', async function () {
			client = new ClientSocket({
				connectTimeoutMs: 500,
				...clientOptions
			});

			assert.strictEqual(client.pingTimeoutMs, 500);

			(async () => {
				for await (const _ of client.listen('connect')) {
					assert.strictEqual(client.pingTimeoutMs, server.pingTimeoutMs);
					// Hack to make the client ping independent from the server ping.
					client.pingTimeoutMs = 500;
				}
			})();

			let closeEvent: CloseEvent | null = null;
			let disconnectEvent: DisconnectEvent | null = null;
			let clientError: Error | null = null;

			(async () => {
				for await (const { error } of client.listen('error')) {
					clientError = error;
				}
			})();

			(async () => {
				for await (const event of client.listen('close')) {
					closeEvent = event;
				}
			})();

			(async () => {
				for await (const event of client.listen('disconnect')) {
					disconnectEvent = event;
				}
			})();

			await wait(1000);

			assert.strictEqual(disconnectEvent!.code, 4000);
			assert.strictEqual(disconnectEvent!.reason, 'Server ping timed out');
			assert.strictEqual(closeEvent!.code, 4000);
			assert.strictEqual(closeEvent!.reason, 'Server ping timed out');
			assert.notEqual(clientError, null);
			assert.strictEqual(clientError!.name, 'SocketProtocolError');
		});

		it('Should not close if ping is not received before timeout when pingTimeoutDisabled is true', async function () {
			client = new ClientSocket({
				connectTimeoutMs: 500,
				isPingTimeoutDisabled: true,
				...clientOptions
			});

			assert.strictEqual(client.pingTimeoutMs, 500);

			let closeEvent: CloseEvent | null = null;
			let disconnectEvent: DisconnectEvent | null = null;
			let clientError: Error | null = null;

			(async () => {
				for await (const { error } of client.listen('error')) {
					clientError = error;
				}
			})();

			(async () => {
				for await (const event of client.listen('close')) {
					closeEvent = event;
				}
			})();

			(async () => {
				for await (const event of client.listen('disconnect')) {
					disconnectEvent = event;
				}
			})();

			await wait(1000);
			assert.strictEqual(clientError, null);
			assert.strictEqual(disconnectEvent, null);
			assert.strictEqual(closeEvent, null);
		});
	});

	describe('Consumable streams', function () {
		it('Should be able to get the stats list of consumers and check if consumers exist on specific channels', async function () {
			client = new ClientSocket(clientOptions);

			const fooChannel = client.channels.channel('foo');

			(async () => {
				for await (const _ of fooChannel.listen('subscribe')) { /* Intentional */ }
			})();
			(async () => {
				for await (const _ of fooChannel.listen('subscribe')) { /* Intentional */ }
			})();
			(async () => {
				for await (const _ of fooChannel.listen('subscribeFail')) { /* Intentional */ }
			})();
			(async () => {
				for await (const _ of fooChannel.listen('customEvent')) { /* Intentional */ }
			})();

			(async () => {
				for await (const _ of client.channels.channel('bar').listen('subscribe')) { /* Intentional */ }
			})();

			const fooStatsList = client.channels.listeners.getConsumerStats('foo');
			const barStatsList = client.channels.listeners.getConsumerStats('bar');

			assert.strictEqual(fooStatsList.length, 4);
			assert.strictEqual(fooStatsList[0]!.id, 1);
			assert.strictEqual(fooStatsList[0]!.stream, 'foo/subscribe');
			assert.strictEqual(fooStatsList[1]!.id, 2);
			assert.strictEqual(fooStatsList[2]!.id, 3);
			assert.strictEqual(fooStatsList[3]!.id, 4);
			assert.strictEqual(fooStatsList[3]!.stream, 'foo/customEvent');

			assert.strictEqual(barStatsList.length, 1);
			assert.strictEqual(barStatsList[0]!.id, 5);
			assert.strictEqual(barStatsList[0]!.stream, 'bar/subscribe');

			assert.strictEqual(client.channels.listeners.hasConsumer('foo', 1), true);
			assert.strictEqual(client.channels.listeners.hasConsumer('foo', 4), true);
			assert.strictEqual(client.channels.listeners.hasConsumer('foo', 5), false);
			assert.strictEqual(client.channels.listeners.hasConsumer('bar', 5), true);
		});

		it('Should be able to check the listener backpressure for specific channels', async function () {
			client = new ClientSocket(clientOptions);

			const fooChannel = client.channels.channel('foo');
			const barChannel = client.channels.channel('bar');
			const fooBackpressures: number[] = [];
			const barBackpressures: number[] = [];

			await Promise.all([
				(async () => {
					for await (const _ of fooChannel.listen('customEvent')) {
						fooBackpressures.push(client.channels.listeners.getBackpressure('foo'));
						await wait(50);
					}
				})(),
				(async () => {
					for await (const _ of barChannel.listen('customEvent')) {
						barBackpressures.push(client.channels.listeners.getBackpressure('bar'));
						await wait(20);
					}
				})(),
				(async () => {
					for (let i = 0; i < 20; i++) {
						(fooChannel as any)._eventDemux.write('foo/customEvent', `message${i}`);
					}
					barChannel.emit('customEvent', `hi0`);
					barChannel.emit('customEvent', `hi1`);
					barChannel.emit('anotherEvent', `hi2`);
					barChannel.closeEvent('customEvent');
					barChannel.closeEvent('anotherEvent');
					fooChannel.closeEvent('customEvent');
				})()
			]);

			assert.strictEqual(fooBackpressures.length, 20);
			assert.strictEqual(fooBackpressures[0], 20);
			assert.strictEqual(fooBackpressures[1], 19);
			assert.strictEqual(fooBackpressures[19], 1);

			assert.strictEqual(barBackpressures.length, 2);
			assert.strictEqual(barBackpressures[0], 2);
			assert.strictEqual(barBackpressures[1], 1);

			assert.strictEqual(client.channels.listeners.getBackpressure('foo'), 0);
			assert.strictEqual(client.channels.listeners.getBackpressure('bar'), 0);
		});

		it('Should be able to kill and close channels and backpressure should update accordingly', async function () {
			client = new ClientSocket(clientOptions);

			await client.listen('connect').once();

			const fooChannel = client.channels.channel('foo');
			const barChannel = client.channels.subscribe('bar');

			await barChannel.listen('subscribe').once(100);

			const fooEvents: string[] = [];
			const barEvents: string[] = [];
			const barMessages: (string | { def: number })[] = [];
			const barBackpressures: number[] = [];
			const allBackpressures: number[] = [];

			await Promise.all([
				(async () => {
					for await (const data of barChannel) {
						await wait(10);
						assert.strictEqual(client.channels.getBackpressure('bar'), barChannel.getBackpressure());
						barBackpressures.push(client.channels.getBackpressure('bar'));
						allBackpressures.push(client.channels.getBackpressure());
						barMessages.push(data);
					}
				})(),
				(async () => {
					for await (const data of fooChannel.listen<string>('customEvent')) {
						fooEvents.push(data);
						await wait(50);
					}
				})(),
				(async () => {
					for await (const data of barChannel.listen<string>('customEvent')) {
						barEvents.push(data);
						await wait(20);
					}
				})(),
				(async () => {
					for (let i = 0; i < 20; i++) {
						fooChannel.emit('customEvent', `message${i}`);
					}
					for (let i = 0; i < 50; i++) {
						barChannel.transmitPublish(`hello${i}`);
					}

					barChannel.emit('customEvent', `hi0`);
					barChannel.emit('customEvent', `hi1`);
					barChannel.emit('customEvent', `hi2`);
					barChannel.emit('customEvent', `hi3`);
					barChannel.emit('customEvent', `hi4`);
					assert.strictEqual(client.channels.getBackpressure('bar'), 5);
					fooChannel.closeEvent('customEvent');
					client.channels.kill('foo');

					await wait(1000);
					assert.strictEqual(client.channels.getBackpressure('bar'), 0);
					client.channels.close('bar');
					assert.strictEqual(client.channels.getBackpressure('bar'), 1);
				})()
			]);

			assert.strictEqual(fooEvents.length, 0);

			assert.strictEqual(barEvents.length, 5);
			assert.strictEqual(barEvents[0], 'hi0');
			assert.strictEqual(barEvents[1], 'hi1');
			assert.strictEqual(barEvents[4], 'hi4');

			assert.strictEqual(barMessages.length, 50);
			assert.strictEqual(barMessages[0], 'hello0');
			assert.strictEqual(barMessages[49], 'hello49');

			assert.strictEqual(client.channels.listeners.getBackpressure('foo'), 0);
			assert.strictEqual(client.channels.listeners.getConsumerStats('bar').length, 0);
			assert.strictEqual(client.channels.listeners.getBackpressure('bar'), 0);

			assert.strictEqual(barBackpressures.length, 50);
			assert.strictEqual(barBackpressures[0], 49);
			assert.strictEqual(barBackpressures[49], 0);

			assert.strictEqual(allBackpressures.length, 50);
			assert.strictEqual(allBackpressures[0], 49);
			assert.strictEqual(allBackpressures[49], 0);
		});
	});
});
