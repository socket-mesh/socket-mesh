/**
 * Wire protocol for cluster-broker traffic between workers and the broker
 * host. Every message is serialized as JSON by a CodecEngine, framed with a
 * 4-byte big-endian length prefix, and sent over a plain TCP connection on
 * loopback.
 *
 * The protocol is intentionally tiny:
 *
 * - `hello`        - worker greets the host and gives itself a stable id so
 *                    the host can log and disambiguate reconnects.
 * - `subscribe`    - worker registers interest in a channel. The host will
 *                    from then on forward publishes on that channel to this
 *                    worker (but skip the originating worker, see below).
 * - `unsubscribe`  - worker drops interest in a channel.
 * - `publish`      - either direction. Worker -> host means "fan this out";
 *                    host -> worker means "here is a message you are
 *                    subscribed to". The host uses the TCP connection the
 *                    publish arrived on as the origin and does NOT echo the
 *                    publish back to that connection, because the worker has
 *                    already delivered the message to its own local
 *                    subscribers before it forwarded it.
 */

export interface HelloMessage {
	type: 'hello',
	workerId: string
}

export interface PublishMessage {
	channel: string,
	data: unknown,
	type: 'publish'
}

export interface SubscribeMessage {
	channel: string,
	type: 'subscribe'
}

export interface UnsubscribeMessage {
	channel: string,
	type: 'unsubscribe'
}

export type WireMessage =
	| HelloMessage
	| PublishMessage
	| SubscribeMessage
	| UnsubscribeMessage;
