import { ChannelMap, Channels, ChannelsOptions } from '@socket-mesh/channels';

interface ExchangeOptions extends ChannelsOptions {
	id: string
}

export abstract class Exchange<T extends ChannelMap> extends Channels<T> {
	id: string;

	constructor(options: ExchangeOptions) {
		super(options);

		this.id = options.id;
	}
}
