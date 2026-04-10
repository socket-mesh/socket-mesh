import { ChannelMap } from '@socket-mesh/channels';
import { RequestHandlerArgs } from '@socket-mesh/core';

import { ClientSocket } from '../client-socket.js';
import { ClientTransport } from '../client-transport.js';
import { KickOutOptions } from '../maps/client-map.js';

export async function kickOutHandler(
	{ options, socket }: RequestHandlerArgs<
		KickOutOptions,
		{},
		ClientSocket<{}, {}, ChannelMap>,
		ClientTransport<{}>
	>
): Promise<void> {
	socket.channels.kickOut(options.channel, options.message);
}
