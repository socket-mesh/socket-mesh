import { AnyRequest, Plugin, SendRequestPluginArgs } from '@socket-mesh/core';

const SYSTEM_METHODS = ['#handshake', '#removeAuthToken'];

export class OfflinePlugin implements Plugin {
	private _continue: ((requests: AnyRequest[], cb?: (error?: Error) => void) => void) | null;
	private _isReady: boolean;
	private _requests: AnyRequest[][];

	type: 'offline';

	constructor() {
		this.type = 'offline';
		this._isReady = false;
		this._requests = [];
		this._continue = null;
	}

	private flush() {
		if (this._requests.length) {
			if (this._continue) {
				for (const reqs of this._requests) {
					this._continue(reqs);
				}

				this._continue = null;
			}

			this._requests = [];
		}
	}

	public onClose(): void {
		this._isReady = false;
	}

	public onDisconnected(): void {
		this._requests = [];
		this._continue = null;
	}

	public onReady(): void {
		this._isReady = true;
		this.flush();
	}

	public sendRequest({ cont, requests }: SendRequestPluginArgs): void {
		if (this._isReady) {
			cont(requests);
			return;
		}

		const systemRequests = requests.filter(item => SYSTEM_METHODS.indexOf(item.method) > -1);
		let otherRequests: AnyRequest[] = requests;

		if (systemRequests.length) {
			otherRequests = (systemRequests.length === requests.length) ? [] : requests.filter(item => SYSTEM_METHODS.indexOf(item.method) < 0);
		}

		if (otherRequests.length) {
			this._continue = cont;
			this._requests.push(otherRequests);
		}

		if (systemRequests.length) {
			cont(systemRequests);
		}
	}
}
