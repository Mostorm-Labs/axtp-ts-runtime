// STREAM codec：16B header = streamId(uint32 BE) + seqId(uint32 BE) + cursor(uint64 BE) + data(N)。
// spec 20-core.md:247 payloadLength MUST == 16 + dataLength（由 frame 层保证 payload 长度）。
// 16B header MUST NOT 携带 codec/file type/metadata/offset/timestamp/flag/domain/event/capability（spec:249）。
// cursor 由 Stream Context 解释，Core 不解释（透传）。

import type { Bytes } from "../../io/bytes.js";
import { ByteReader, ByteWriter } from "../../io/io.js";
import type { StreamPayload } from "../model.js";

export const kStreamHeaderSize = 16;

/** 编码 STREAM payload（message body = 16B header + data）。 */
export function encodeStream(payload: StreamPayload): Bytes {
  const writer = new ByteWriter();
  writer.writeU32(payload.streamId);
  writer.writeU32(payload.seqId);
  writer.writeU64(payload.cursor);
  writer.writeBytes(payload.data);
  return writer.takeBytes();
}

/** 解码 STREAM message body -> StreamPayload。返回 undefined 表示非法（header 不足 16B）。 */
export function decodeStream(body: Bytes): StreamPayload | undefined {
  if (body.length < kStreamHeaderSize) return undefined;
  const reader = new ByteReader(body);
  const streamId = reader.readU32Strict();
  if (streamId === 0) return undefined; // spec:251 MUST 校验 streamId != 0
  const seqId = reader.readU32Strict();
  const cursor = reader.readU64Strict();
  const data = reader.readBytesStrict(reader.remaining());
  return { streamId, seqId, cursor, data };
}
