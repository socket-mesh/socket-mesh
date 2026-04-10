import { SignedAuthToken } from '@socket-mesh/auth';
import { ChannelMap } from '@socket-mesh/channels';
import { FunctionReturnType, InvokeMethodOptions, InvokeServiceOptions, MethodMap, PrivateMethodMap, PublicMethodMap, ServiceMap, ServiceMethodName, ServiceName, Socket, toError, wait } from '@socket-mesh/core';
import { hydrateError } from '@socket-mesh/errors';

import { ClientChannels } from './client-channels.js';
import { AutoReconnectOptions, ClientSocketOptions, ConnectOptions, parseClientOptions } from './client-socket-options.js';
import { ClientTransport } from './client-transport.js';
import { kickOutHandler } from './handlers/kickout.js';
import { publishHandler } from './handlers/publish.js';
import { removeAuthTokenHandler } from './handlers/remove-auth-token.js';
import { setAuthTokenHandler } from './handlers/set-auth-token.js';
import { ClientPrivateMap } from './maps/client-map.js';
import { ServerPrivateMap } from './maps/server-map.js';

export class ClientSocket<
	TOutgoing extends PublicMethodMap = {},
	TChannel extends ChannelMap = ChannelMap,
	TService extends ServiceMap = {},
	TState extends object = {},
	TIncoming extends MethodMap = {},
	TPrivateOutgoing extends PrivateMethodMap = {}
> extends Socket<
	TIncoming & ClientPrivateMap,
	TOutgoing,
	TPrivateOutgoing & ServerPrivateMap,
	TService,
	TState
	> {
	private readonly _clientTransport: ClientTransport<TIncoming, TService, TOutgoing, TPrivateOutgoing, TState>;
	public readonly channels: ClientChannels<TChannel, TIncoming, TService, TOutgoing, TPrivateOutgoing, TState>;

	constructor(address: string | URL);
	constructor(options: ClientSocketOptions<TOutgoing, TService, TIncoming, TPrivateOutgoing, TState>);
	constructor(options: ClientSocketOptions<TOutgoing, TService, TIncoming, TPrivateOutgoing, TState> | string | URL) {
		options = parseClientOptions(options);

		options.handlers =
			Object.assign(
				{
					'#kickOut': kickOutHandler,
					'#publish': publishHandler,
					'#removeAuthToken': removeAuthTokenHandler,
					'#setAuthToken': setAuthTokenHandler
				},
				options.handlers
			);

		const clientTransport = new ClientTransport(options);

		super(clientTransport, options);

		this._clientTransport = clientTransport;
		this.channels = new ClientChannels<TChannel, TIncoming, TService, TOutgoing, TPrivateOutgoing, TState>(this._clientTransport, options);

		if (options.autoConnect !== false) {
			this.connect(options);
		}
	}

	public async authenticate(signedAuthToken: SignedAuthToken): Promise<void> {
		try {
			await this._clientTransport.invoke('#authenticate', signedAuthToken)[0];

			this._clientTransport.setAuthorization(signedAuthToken);

			// In order for the events to trigger we need to wait for the next tick.
			await wait(0);
		} catch (err) {
			const error = toError(err);

			if (error.name !== 'BadConnectionError' && error.name !== 'TimeoutError') {
				// In case of a bad/closed connection or a timeout, we maintain the last
				// known auth state since those errors don't mean that the token is invalid.
				await this._clientTransport.changeToUnauthenticatedState();

				// In order for the events to trigger we need to wait for the next tick.
				await wait(0);
			}

			throw hydrateError(error);
		}
	}

	public get autoReconnect(): AutoReconnectOptions | false {
		return this._clientTransport.autoReconnect;
	}

	public set autoReconnect(value: boolean | Partial<AutoReconnectOptions>) {
		this._clientTransport.autoReconnect = value;
	}

	public connect(options?: ConnectOptions): void {
		this._clientTransport.connect(options);
	}

	public get connectTimeoutMs(): number {
		return this._clientTransport.connectTimeoutMs;
	}

	public set connectTimeoutMs(timeoutMs: number) {
		this._clientTransport.connectTimeoutMs = timeoutMs;
	}

	async deauthenticate(): Promise<boolean> {
		(async () => {
			let oldAuthToken: null | SignedAuthToken;

			try {
				oldAuthToken = await this._clientTransport.authEngine.removeToken();
			} catch (err) {
				this._clientTransport.onError(toError(err));
				return;
			}

			if (oldAuthToken) {
				this.emit('removeAuthToken', { oldAuthToken });
			}
		})();

		if (this.status !== 'closed') {
			await this._clientTransport.transmit('#removeAuthToken');
		}

		return await super.deauthenticate();
	}

	override invoke<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod, (false | number)?], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: InvokeServiceOptions<TService, TServiceName, TMethod>, arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<FunctionReturnType<TService[TServiceName][TMethod]>>;
	override invoke<TMethod extends keyof TOutgoing & string>(options: InvokeMethodOptions<TOutgoing, TMethod>, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<FunctionReturnType<TOutgoing[TMethod]>>;
	override invoke(
		methodOptions: [string, string, (false | number)?] | InvokeMethodOptions | InvokeServiceOptions | string,
		arg?: unknown
	): Promise<unknown> {
		return super.invoke(methodOptions, arg);
	}

	public get isPingTimeoutDisabled(): boolean {
		return this._clientTransport.isPingTimeoutDisabled;
	}

	public set isPingTimeoutDisabled(isDisabled: boolean) {
		this._clientTransport.isPingTimeoutDisabled = isDisabled;
	}

	public get pingTimeoutMs(): number {
		return this._clientTransport.pingTimeoutMs;
	}

	public set pingTimeoutMs(timeoutMs: number) {
		this._clientTransport.pingTimeoutMs = timeoutMs;
	}

	public reconnect(code?: number, reason?: string) {
		this.disconnect(code, reason);
		this.connect();
	}

	override transmit<TMethod extends keyof TOutgoing & string>(method: TMethod, arg?: Parameters<TOutgoing[TMethod]>[0]): Promise<void>;
	override transmit<TServiceName extends ServiceName<TService>, TMethod extends ServiceMethodName<TService, TServiceName>>(options: [TServiceName, TMethod], arg?: Parameters<TService[TServiceName][TMethod]>[0]): Promise<void>;
	override transmit(
		serviceAndMethod: [string, string] | string,
		arg?: unknown
	): Promise<void> {
		return super.transmit(serviceAndMethod, arg);
	}

	get type(): 'client' {
		return this._clientTransport.type;
	}

	public get uri(): URL {
		return this._clientTransport.uri;
	}
}
