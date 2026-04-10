import { ChannelMap, PublishOptions } from '@socket-mesh/channels';
import { RequestHandlerArgs } from '@socket-mesh/core';

import { ClientSocket } from '../client-socket.js';
import { ClientTransport } from '../client-transport.js';

export async function publishHandler(
	{ options, socket }: RequestHandlerArgs<
		PublishOptions,
		{},
		ClientSocket<{}, ChannelMap>,
		ClientTransport<{}>
	>
): Promise<void> {
	socket.channels.write(options.channel, options.data);
}
