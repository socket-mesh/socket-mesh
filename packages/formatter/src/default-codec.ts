import type { CodecEngine } from './codec-engine.js';

const BASE_64_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';

const validJsonStartRegex = /^[ \n\r\t]*[{[]/;

function arrayBufferToBase64(arraybuffer: ArrayBuffer): string {
	const bytes = new Uint8Array(arraybuffer);
	const len = bytes.length;
	let base64 = '';

	for (let i = 0; i < len; i += 3) {
		base64 += BASE_64_CHARS[bytes[i]! >> 2];
		base64 += BASE_64_CHARS[((bytes[i]! & 3) << 4) | (bytes[i + 1]! >> 4)];
		base64 += BASE_64_CHARS[((bytes[i + 1]! & 15) << 2) | (bytes[i + 2]! >> 6)];
		base64 += BASE_64_CHARS[bytes[i + 2]! & 63];
	}

	if ((len % 3) === 2) {
		base64 = base64.substring(0, base64.length - 1) + '=';
	} else if (len % 3 === 1) {
		base64 = base64.substring(0, base64.length - 2) + '==';
	}

	return base64;
}

function binaryToBase64Replacer(key: string, value: any): any {
	if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
		return {
			base64: true,
			data: arrayBufferToBase64(value)
		};
	} else if (typeof Buffer !== 'undefined') {
		if (value instanceof Buffer) {
			return {
				base64: true,
				data: value.toString('base64')
			};
		}
		// Some versions of Node.js convert Buffers to Objects before they are passed to
		// the replacer function - Because of this, we need to rehydrate Buffers
		// before we can convert them to base64 strings.
		if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
			const rehydratedBuffer = Buffer.from(value.data);

			return {
				base64: true,
				data: rehydratedBuffer.toString('base64')
			};
		}
	}
	return value;
}

class DefaultCodec implements CodecEngine {
	// Decode the data which was transmitted over the wire to a JavaScript Object in a format which SC understands.
	// See encode function below for more details.
	decode(encodedMessage: null | string): any {
		if (encodedMessage == null) {
			return null;
		}
		// Leave ping or pong message as is
		if (encodedMessage === '#1' || encodedMessage === '#2') {
			return encodedMessage;
		}
		const message = encodedMessage.toString();

		// Performance optimization to detect invalid JSON packet sooner.
		if (!validJsonStartRegex.test(message)) {
			return message;
		}

		try {
			return JSON.parse(message);
		} catch (err) {
			// On purpose
		}

		return message;
	}

	// Encode raw data (which is in the SM protocol format) into a format for
	// transfering it over the wire. In this case, we just convert it into a simple JSON string.
	// If you want to create your own custom codec, you can encode the object into any format
	// (e.g. binary ArrayBuffer or string with any kind of compression) so long as your decode
	// function is able to rehydrate that object back into its original JavaScript Object format
	// (which adheres to the SM protocol).
	// See https://github.com/SocketCluster/socketcluster/blob/master/socketcluster-protocol.md
	// for details about the SM protocol.
	encode(rawData: any): string {
		// Leave ping or pong message as is
		if (rawData === '#1' || rawData === '#2') {
			return rawData;
		}
		return JSON.stringify(rawData, binaryToBase64Replacer);
	}
}

const DefaultCodecInstance = new DefaultCodec();

export default DefaultCodecInstance;
