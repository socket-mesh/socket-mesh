import { CodecEngine } from '@socket-mesh/formatter';

import { WireMessage } from './wire.js';

/**
 * Length-prefixed framing for wire messages over a stream socket. Each
 * frame is `<4-byte big-endian length><utf-8 payload>` where the payload
 * is the codec-encoded wire message.
 *
 * The buffered decoder tolerates fragmented reads (a single `data`
 * event from node's `net` may contain a partial frame, multiple frames,
 * or both). It accumulates bytes until at least one full frame is
 * available and then drains as many complete frames as possible,
 * yielding each decoded message to the caller.
 */

const LENGTH_PREFIX_BYTES = 4;
const MAX_FRAME_BYTES = 16 * 1024 * 1024;

export function encodeFrame(codec: CodecEngine, message: WireMessage): Buffer {
	const payload = Buffer.from(codec.encode(message), 'utf8');

	if (payload.length > MAX_FRAME_BYTES) {
		throw new Error(
			`cluster-broker frame exceeds max size (${payload.length} > ${MAX_FRAME_BYTES} bytes)`
		);
	}

	const frame = Buffer.allocUnsafe(LENGTH_PREFIX_BYTES + payload.length);

	frame.writeUInt32BE(payload.length, 0);
	payload.copy(frame, LENGTH_PREFIX_BYTES);

	return frame;
}

export class FrameDecoder {
	private _buffer: Buffer;
	private readonly _codec: CodecEngine;

	constructor(codec: CodecEngine) {
		this._codec = codec;
		this._buffer = Buffer.alloc(0);
	}

	push(chunk: Buffer): WireMessage[] {
		this._buffer = this._buffer.length === 0
			? chunk
			: Buffer.concat([this._buffer, chunk]);

		const messages: WireMessage[] = [];

		while (this._buffer.length >= LENGTH_PREFIX_BYTES) {
			const frameLength = this._buffer.readUInt32BE(0);

			if (frameLength > MAX_FRAME_BYTES) {
				throw new Error(
					`cluster-broker frame exceeds max size (${frameLength} > ${MAX_FRAME_BYTES} bytes)`
				);
			}

			const frameEnd = LENGTH_PREFIX_BYTES + frameLength;

			if (this._buffer.length < frameEnd) {
				break;
			}

			const payload = this._buffer.subarray(LENGTH_PREFIX_BYTES, frameEnd).toString('utf8');

			this._buffer = this._buffer.subarray(frameEnd);

			messages.push(this._codec.decode(payload) as WireMessage);
		}

		return messages;
	}
}
