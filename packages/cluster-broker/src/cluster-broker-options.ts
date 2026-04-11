import { CodecEngine } from '@socket-mesh/formatter';

export interface ClusterBrokerHostOptions {
	/** Codec used for wire frames. Defaults to the JSON codec. */
	codecEngine?: CodecEngine,

	/** Bind address for the TCP listener. Defaults to `127.0.0.1`. */
	host?: string,

	/** TCP port to listen on. Required. */
	port: number
}

export interface ClusterBrokerOptions {
	/**
	 * Codec used to serialize wire frames. Defaults to the shared JSON
	 * codec from `@socket-mesh/formatter` when omitted. Workers and host
	 * must agree on the same codec.
	 */
	codecEngine?: CodecEngine,

	/** Host ip/hostname of the broker process. Defaults to `127.0.0.1`. */
	host?: string,

	/** TCP port the broker host is listening on. */
	port: number,

	/**
	 * Base delay between reconnect attempts when the underlying TCP
	 * connection drops. The effective delay backs off exponentially up
	 * to a 30s cap. Defaults to 200ms.
	 */
	reconnectBaseDelayMs?: number,

	/**
	 * Optional stable identifier for this worker. Used only for
	 * diagnostics; the host identifies workers by connection, not by
	 * this id. Defaults to `worker-<pid>`.
	 */
	workerId?: string
}
