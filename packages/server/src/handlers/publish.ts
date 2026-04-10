import { PublishOptions } from '@socket-mesh/channels';
import { InvalidActionError } from '@socket-mesh/errors';

import { ServerRequestHandlerArgs } from './server-request-handler.js';

export async function publishHandler(
	{ options, socket, transport }: ServerRequestHandlerArgs<PublishOptions, {}, {}, { [channel: string]: any }>
): Promise<void> {
	if (!socket.server.allowClientPublish) {
		throw new InvalidActionError('Client publish feature is disabled');
	}

	if (typeof options.channel !== 'string' || !options.data) {
		throw new InvalidActionError('Publish channel name was malformatted');
	}

	let data = options.data;

	for (const plugin of socket.server.plugins) {
		if (plugin.onPublishIn) {
			data = await plugin.onPublishIn({
				channel: options.channel,
				data,
				socket,
				transport
			});
		}
	}

	await socket.server.exchange.invokePublish(options.channel, data);
}
