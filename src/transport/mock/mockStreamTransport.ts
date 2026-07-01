// Mock stream transport（内存 loopback，供 SDK 测试 + conformance）。
// 用自定义 ReadableStream + WritableStream 手工对接（避免 identity TransformStream 在 vitest 下的
// transformAlgorithm realm 问题）。createMockStreamPair 返回直接对接的两端；createMockStreamLoopback
// 提供 client.connect / server.onConnection 的 AxtpClient/AxtpServer 友好形态。

import type { Bytes } from "../../io/bytes.js";
import { EventStream } from "../../types/events.js";
import type {
  StreamClientTransport,
  StreamServerTransport,
  StreamTransport,
  TransportProfile
} from "../contract.js";
import { framedBinaryProfile } from "../profile.js";

/** 创建一对直接对接的 stream transport（A.writable→B.readable，B.writable→A.readable）。 */
export function createMockStreamPair(
  profile: TransportProfile = framedBinaryProfile("AXTP-TCP")
): [StreamTransport, StreamTransport] {
  let c1: ReadableStreamDefaultController<Bytes> | undefined;
  let c2: ReadableStreamDefaultController<Bytes> | undefined;
  const r1 = new ReadableStream<Bytes>({
    start: (c) => {
      c1 = c;
    }
  });
  const r2 = new ReadableStream<Bytes>({
    start: (c) => {
      c2 = c;
    }
  });
  // w1.write → c2.enqueue（B 读 r2）；w1.close → c2.close（B readable 结束）。
  // enqueue 经 queueMicrotask 延后：避免同步 write→enqueue 在 Web Streams pipe 链中引发重入
  // （真实 transport 的字节到达是异步的；同步模拟会破坏 pipe 的内部状态机）。
  const w1 = new WritableStream<Bytes>({
    write: (chunk) => {
      queueMicrotask(() => {
        try {
          c2?.enqueue(chunk);
        } catch {
          /* 已关闭 */
        }
      });
    },
    close: () => {
      queueMicrotask(() => {
        try {
          c2?.close();
        } catch {
          /* 已关闭 */
        }
      });
    },
    abort: () => {
      queueMicrotask(() => {
        try {
          c2?.close();
        } catch {
          /* 已关闭 */
        }
      });
    }
  });
  const w2 = new WritableStream<Bytes>({
    write: (chunk) => {
      queueMicrotask(() => {
        try {
          c1?.enqueue(chunk);
        } catch {
          /* 已关闭 */
        }
      });
    },
    close: () => {
      queueMicrotask(() => {
        try {
          c1?.close();
        } catch {
          /* 已关闭 */
        }
      });
    },
    abort: () => {
      queueMicrotask(() => {
        try {
          c1?.close();
        } catch {
          /* 已关闭 */
        }
      });
    }
  });
  const mk = (
    readable: ReadableStream<Bytes>,
    writable: WritableStream<Bytes>
  ): StreamTransport => ({
    profile,
    readable,
    writable,
    close: () => {
      writable.close().catch(() => {});
    },
    terminate: () => {
      writable.abort().catch(() => {});
    }
  });
  return [mk(r1, w1), mk(r2, w2)];
}

export interface MockStreamLoopback {
  readonly client: StreamClientTransport;
  readonly server: StreamServerTransport;
}

/** client.connect / server.onConnection 形态的 loopback（AxtpClient/AxtpServer 友好）。每次 connect 新建一对。 */
export function createMockStreamLoopback(
  profile: TransportProfile = framedBinaryProfile("AXTP-TCP")
): MockStreamLoopback {
  const onConnection = new EventStream<StreamTransport>();
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  let listening = false;
  return {
    client: {
      profile,
      connect: () =>
        new Promise<StreamTransport>((resolve) => {
          const [clientT, serverT] = createMockStreamPair(profile);
          onConnection.emit(serverT);
          resolve(clientT);
        })
    },
    server: {
      profile,
      listen: async () => {
        listening = true;
      },
      onConnection,
      close: async () => {
        listening = false;
        onConnection.close();
      }
    }
  };
}
