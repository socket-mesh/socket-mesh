import { MessageRawPluginArgs, Plugin, PluginArgs } from '@socket-mesh/core';
import { WritableConsumableStream } from '@socket-mesh/writable-consumable-stream';
import ws from 'isomorphic-ws';
import { RawData } from 'ws';

interface InboundMessage extends MessageRawPluginArgs {
	callback: (err: Error | null, data: string | ws.RawData) => void
}

export class InOrderPlugin implements Plugin {
	private readonly _inboundMessageStream: WritableConsumableStream<InboundMessage>;

	type: 'inOrder';
	// private readonly _outboundMessageStream: WritableConsumableStream<SendRequestPluginArgs<T>>;

	constructor() {
		this._inboundMessageStream = new WritableConsumableStream<InboundMessage>();
		// this._outboundMessageStream = new WritableConsumableStream<SendRequestPluginArgs<T>>;
		this.type = 'inOrder';
		this.handleInboundMessageStream();
		// this.handleOutboundMessageStream();
	}

	handleInboundMessageStream(): void {
		(async () => {
			for await (const { callback, message, promise } of this._inboundMessageStream) {
				callback(null, message);
				try {
					await promise;
				} catch (err) {
					// Dont throw it is handled in the socket transport
				}
			}
		})();
	}

	/*
	handleOutboundMessageStream(): void {
		(async () => {
			for await (let { requests, cont } of this._outboundMessageStream) {
				await new Promise<void>((resolve) => {
					const reqCol = new RequestCollection(requests);

					if (reqCol.isDone()) {
						resolve();
						cont(requests);
						return;
					}

					reqCol.listen(() => {
						resolve();
					});

					cont(requests);
				});
			}
		})();
	}
*/

	onEnd({ transport }: PluginArgs): void {
		if (transport.streamCleanupMode === 'close') {
			this._inboundMessageStream.close();
			// this._outboundMessageStream.close();
		} else if (transport.streamCleanupMode === 'kill') {
			this._inboundMessageStream.kill();
			// this._outboundMessageStream.kill();
		}
	}

	onMessageRaw(options: MessageRawPluginArgs): Promise<RawData | string> {
		let callback: (err: Error | null, data: string | ws.RawData) => void;

		const promise = new Promise<RawData | string>((resolve, reject) => {
			callback = (err, data) => {
				if (err) {
					reject(err);
					return;
				}

				resolve(data);
			};
		});

		this._inboundMessageStream.write({ callback: callback!, ...options });

		return promise;
	}

	// sendRequest(options: SendRequestPluginArgs<T>): void {
	// this._outboundMessageStream.write(options);
	// }
}
