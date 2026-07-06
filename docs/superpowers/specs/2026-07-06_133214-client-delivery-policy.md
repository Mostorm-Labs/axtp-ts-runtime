# AXTP Client Delivery Policy Spec

> 保存位置：`docs/superpowers/specs/2026-07-06_133214-client-delivery-policy.md`  
> 来源：对「每次 call RPC 显式设定太麻烦」以及「outbox 和 call options 是否可合并」brainstorming 的分析与规格化。

## 1. 背景与问题

当前 `AxtpClient` 已经支持：

- event outbox：`ClientOptions.outbox`
- RPC 单次策略：`CallOptions.offlinePolicy`
- RPC queue 容量：`ClientOptions.calls.queue`

代码现状主要分布在：

- `src/sdk/client.ts`
  - `ClientOutboxOptions`
  - `ClientCallsOptions`
  - `ClientOptions.outbox`
  - `ClientOptions.calls.queue`
  - `callRaw()` 对 `options.offlinePolicy` 的分支处理
  - `enqueueEvent()` 使用 `options.outbox`
  - `enqueueCall()` 使用 `options.calls?.queue`
- `src/sdk/types.ts`
  - `CallOptions`
  - `CallOfflinePolicy`
  - `QueuedCallCoalescePrevious`
- `docs/usage.md`
  - 当前文档强调 event outbox 和 RPC offline policy 是两套配置入口。
- `tests/sdk/sdk.test.ts`
  - 已覆盖 outbox、wait-ready、queue、coalesce、call queue overflow 等行为。

### 当前 API 痛点

#### 痛点 1：安全 RPC 每次都要显式传 `CallOptions`

现在若用户希望大量 safe/idempotent RPC 在连接未 ready 时等待，需要每次写：

```ts
await client.callRaw(
  "device.getInfo",
  {},
  {
    offlinePolicy: "wait-ready",
    timeoutMs: 5_000
  }
);
```

这使得调用点重复、容易漏写，也很难统一调整策略。

#### 痛点 2：通信策略入口分散

当前配置入口分散为：

```ts
new AxtpClient(transport, {
  outbox: { enabled: true },
  calls: {
    queue: { maxSize: 1000, overflow: "reject" }
  }
});

await client.callRaw("foo", {}, { offlinePolicy: "queue" });
```

问题：

1. `outbox` 名字看似通用，但实际只管 event。
2. `calls.queue` 只管 RPC queue 容量，不管默认 `offlinePolicy`。
3. `CallOptions` 只能做单次 override，不能表达 client / method 级默认策略。

## 2. 外部模式参考与结论

### gRPC 的启发

类似问题在 gRPC 中通常不是每次调用手写策略，而是采用分层配置：

- channel / client default
- service config
- method config
- per-call override

其中 `wait-for-ready` 与 AXTP 的 `offlinePolicy: "wait-ready"` 语义相近：

- 未 ready 时不立即失败，而是等待 channel ready。
- deadline / timeout 仍应保留边界。
- 策略可以在 method config 层配置，而不是每次 call 手写。

映射到 AXTP：

```ts
new AxtpClient(transport, {
  delivery: {
    calls: {
      default: {
        offlinePolicy: "wait-ready",
        timeoutMs: 5_000
      },
      methods: {
        "device.getInfo": {
          offlinePolicy: "wait-ready"
        },
        "device.factoryReset": {
          offlinePolicy: "fail-fast"
        }
      }
    }
  }
});
```

### Retry / queue 的安全原则

RPC 自动等待、重试、queue 的风险取决于幂等性：

1. 请求可能已经到达服务端并产生副作用。
2. 响应可能在返回途中丢失。
3. 客户端若自动 replay，可能造成副作用重复执行。

因此 AXTP 应继续保持：

- 默认安全：`fail-fast`
- 用户显式声明哪些 method 可以 `wait-ready` / `queue` / coalesce
- 不由 SDK 猜测 method 是否幂等

### Outbox pattern 的边界

传统 outbox 更适合 event / message：

- event 是 fire-and-forget。
- event 没有 AXTP 层响应。
- 重复投递可以由 consumer 幂等处理。

RPC queue 则语义更复杂：

- 有 request-response promise。
- 需要区分“尚未写入 endpoint”与“已写入但断开”。
- coalesce 时 previous promise 如何 resolve/reject 是 RPC 特有问题。

结论：

> outbox 和 CallOptions 概念上都属于通信交付策略，但类型层不应粗暴合并成一个扁平对象。应上提到统一 `delivery` namespace，并保留 `events` / `calls` / `streams` 子策略。

## 3. 目标

新增 `ClientOptions.delivery`，用于统一描述客户端发送/交付策略：

```ts
new AxtpClient(transport, {
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1000, overflow: "reject" }
    },
    calls: {
      default: {
        offlinePolicy: "wait-ready",
        timeoutMs: 5_000
      },
      queue: { maxSize: 1000, overflow: "reject" },
      methods: {
        "cast.setAirPlayName": {
          offlinePolicy: "queue",
          coalesceKey: "cast.setAirPlayName",
          coalescePrevious: "resolve-with-next"
        },
        "device.factoryReset": {
          offlinePolicy: "fail-fast"
        }
      }
    }
  }
});
```

实现后：

```ts
await client.callRaw("device.getInfo", {});
```

可继承 `delivery.calls.default`，不再要求每次显式传：

```ts
{ offlinePolicy: "wait-ready", timeoutMs: 5_000 }
```

## 4. 非目标

本 spec 不要求：

- 自动判断 RPC 是否幂等。
- 自动 replay 已经写入 endpoint 的 RPC。
- 持久化 outbox / RPC queue 到磁盘。
- 改变默认安全行为。
- 删除旧 `outbox` / `calls.queue` API。
- 一次性完成 stream delivery policy。

## 5. API 设计

### 5.1 复用现有基础类型

现有：

```ts
export type CallOfflinePolicy = "fail-fast" | "wait-ready" | "queue";
export type QueuedCallCoalescePrevious = "resolve-with-next" | "reject" | "drop";

export interface CallOptions {
  timeoutMs?: number;
  offlinePolicy?: CallOfflinePolicy;
  coalesceKey?: string;
  coalescePrevious?: QueuedCallCoalescePrevious;
}
```

### 5.2 新增通用 queue 类型

建议在 `src/sdk/client.ts` 或更合适的 SDK public types 文件中新增：

```ts
export interface ClientDeliveryQueueOptions {
  maxSize?: number;
  overflow?: "drop-oldest" | "drop-newest" | "reject";
}
```

也可以保留 `ClientCallQueueOptions`，但长期建议收敛为同一个 queue 类型。

### 5.3 Event delivery policy

```ts
export interface EventDeliveryPolicy {
  /**
   * fail-fast: emit while not ready rejects.
   * queue: emit while not ready is queued in memory and flushed after ready.
   */
  offline?: "fail-fast" | "queue";
  queue?: ClientDeliveryQueueOptions;
}
```

语义：

- `offline: "fail-fast"` 等价于当前 `outbox.enabled !== true`。
- `offline: "queue"` 等价于当前 `outbox.enabled === true`。
- `queue.maxSize` / `queue.overflow` 等价于当前 `outbox.maxSize` / `outbox.overflow`。

### 5.4 Call delivery policy

```ts
export interface CallDeliveryDefaults {
  timeoutMs?: number;
  offlinePolicy?: CallOfflinePolicy;
}

export interface CallMethodDeliveryPolicy extends CallDeliveryDefaults {
  coalesceKey?: string;
  coalescePrevious?: QueuedCallCoalescePrevious;
}

export interface CallDeliveryPolicy {
  default?: CallDeliveryDefaults;
  queue?: ClientDeliveryQueueOptions;
  methods?: Record<string, CallMethodDeliveryPolicy>;
}
```

### 5.5 Client delivery options

```ts
export interface ClientDeliveryOptions {
  events?: EventDeliveryPolicy;
  calls?: CallDeliveryPolicy;
  /** reserved for future; do not implement behavior in this spec */
  streams?: unknown;
}
```

### 5.6 ClientOptions 扩展

```ts
export interface ClientOptions {
  logicalRole?: LogicalRole;
  defaultTimeoutMs?: number;
  handshakeTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  maxFrameSize?: number;
  reconnect?: ReconnectPolicy;

  delivery?: ClientDeliveryOptions;
  diagnostics?: AxtpDiagnostics;
}
```

## 6. 策略解析规则

### 6.1 RPC call option 优先级

最终 `ResolvedCallOptions` 应按以下优先级合并：

```text
per-call options
> delivery.calls.methods[method]
> delivery.calls.default
> SDK defaults
```

推荐实现：

```ts
private resolveCallOptions(method: string, options?: CallOptions): CallOptions {
  return {
    ...this.options.delivery?.calls?.default,
    ...this.resolveMethodCallPolicy(method),
    ...options
  };
}
```

### 6.2 method policy 匹配

#### MVP

MVP 只支持精确 method name：

```ts
methods: {
  "device.getInfo": { offlinePolicy: "wait-ready" }
}
```

#### 可选增强

后续可考虑支持通配符：

```ts
methods: {
  "dangerous.*": { offlinePolicy: "fail-fast" }
}
```

但本 spec 的第一版建议不实现通配符，避免引入匹配顺序、冲突优先级等复杂度。

### 6.3 RPC queue options 优先级

`enqueueCall()` 中 queue 容量策略优先级：

```text
delivery.calls.queue
> SDK default { maxSize: 1000, overflow: "reject" }
```

### 6.4 Event delivery 优先级

event offline 策略优先级：

```text
delivery.events
> SDK default fail-fast
```

映射规则：

```ts
private resolveEventDeliveryPolicy(): Required<EventDeliveryPolicy> {
  const eventPolicy = this.options.delivery?.events;
  return {
    offline: eventPolicy?.offline ?? "fail-fast",
    queue: {
      maxSize: eventPolicy?.queue?.maxSize ?? 1000,
      overflow: eventPolicy?.queue?.overflow ?? "reject"
    }
  };
}
```

## 7. 行为规格

### 7.1 默认行为必须保持不变

未配置 `delivery` 时：

```ts
new AxtpClient(transport);
```

行为保持：

- event not ready：fail-fast
- call not ready：fail-fast
- call timeout：沿用 `defaultTimeoutMs` / endpoint 默认逻辑
- RPC queue maxSize：1000
- RPC queue overflow：`reject`

### 7.2 `delivery.events.offline = "queue"`

配置：

```ts
new AxtpClient(transport, {
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1, overflow: "drop-oldest" }
    }
  }
});
```

这是旧 `outbox: { enabled: true, maxSize: 1, overflow: "drop-oldest" }` 的 replacement；旧写法在本 breaking change 中不再支持。

### 7.3 `delivery.calls.default.offlinePolicy = "wait-ready"`

配置：

```ts
new AxtpClient(transport, {
  delivery: {
    calls: {
      default: { offlinePolicy: "wait-ready", timeoutMs: 5_000 }
    }
  }
});
```

调用：

```ts
const p = client.callRaw("add", { a: 4, b: 6 });
await client.connect();
await expect(p).resolves.toBe(10);
```

应等价于旧写法：

```ts
client.callRaw(
  "add",
  { a: 4, b: 6 },
  {
    offlinePolicy: "wait-ready",
    timeoutMs: 5_000
  }
);
```

### 7.4 method policy 覆盖 default

配置：

```ts
new AxtpClient(transport, {
  delivery: {
    calls: {
      default: { offlinePolicy: "wait-ready" },
      methods: {
        "dangerous.deleteFile": { offlinePolicy: "fail-fast" }
      }
    }
  }
});
```

行为：

- `device.getInfo` 未 ready 时 wait-ready。
- `dangerous.deleteFile` 未 ready 时 fail-fast。

### 7.5 per-call options 覆盖 method policy

配置：

```ts
new AxtpClient(transport, {
  delivery: {
    calls: {
      default: { offlinePolicy: "wait-ready" }
    }
  }
});
```

调用：

```ts
await client.callRaw("device.getInfo", {}, { offlinePolicy: "fail-fast" });
```

应立即 fail-fast，不等待 ready。

### 7.6 method-level queue/coalesce

配置：

```ts
new AxtpClient(transport, {
  delivery: {
    calls: {
      methods: {
        "cast.setAirPlayName": {
          offlinePolicy: "queue",
          coalesceKey: "cast.setAirPlayName",
          coalescePrevious: "resolve-with-next"
        }
      }
    }
  }
});
```

调用：

```ts
const first = client.callRaw("cast.setAirPlayName", { name: "Room A" });
const second = client.callRaw("cast.setAirPlayName", { name: "Room B" });
const third = client.callRaw("cast.setAirPlayName", { name: "Room C" });
```

行为应等价于每次显式传同一组 `CallOptions`：

- 只发送最后一次 `{ name: "Room C" }`。
- `first` / `second` / `third` 都 resolve 为最后一次调用的结果。

## 8. Breaking Change 与迁移

### 8.1 直接删除旧 API

本设计选择 breaking change：不再保留旧配置入口。

删除：

```ts
outbox?: ClientOutboxOptions;
calls?: ClientCallsOptions;
ClientOutboxOptions;
ClientCallsOptions;
ClientCallQueueOptions;
```

保留并推荐：

```ts
delivery?: ClientDeliveryOptions;
```

迁移规则：

```text
outbox.enabled/maxSize/overflow -> delivery.events.offline/queue
calls.queue                  -> delivery.calls.queue
per-call CallOptions          -> 保留，继续作为单次 override
```

### 8.2 迁移示例

旧写法：

```ts
new AxtpClient(transport, {
  outbox: { enabled: true, maxSize: 1000, overflow: "reject" },
  calls: { queue: { maxSize: 1000, overflow: "reject" } }
});
```

新写法：

```ts
new AxtpClient(transport, {
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1000, overflow: "reject" }
    },
    calls: {
      queue: { maxSize: 1000, overflow: "reject" }
    }
  }
});
```

### 8.3 删除兼容 fallback

实现不应写成：

```ts
this.options.delivery?.calls?.queue ?? this.options.calls?.queue;
```

而应只读取：

```ts
this.options.delivery?.calls?.queue;
```

同理，event delivery 不应再读取 `this.options.outbox`。旧配置在 TypeScript 层直接报错，让调用方显式迁移。

## 9. 实施建议

### Step 1：补类型

修改：`src/sdk/client.ts` / `src/sdk/types.ts`

新增：

- `ClientDeliveryQueueOptions`
- `EventDeliveryPolicy`
- `CallDeliveryDefaults`
- `CallMethodDeliveryPolicy`
- `CallDeliveryPolicy`
- `ClientDeliveryOptions`

并在 `ClientOptions` 增加 `delivery?: ClientDeliveryOptions`。

### Step 2：补 resolver

修改：`src/sdk/client.ts`

新增私有 helper：

```ts
private resolveCallOptions(method: string, options?: CallOptions): CallOptions {
  return {
    ...this.options.delivery?.calls?.default,
    ...this.options.delivery?.calls?.methods?.[method],
    ...options
  };
}
```

新增 event policy helper：

```ts
private resolveEventDeliveryPolicy(): {
  offline: "fail-fast" | "queue";
  queue: Required<ClientDeliveryQueueOptions>;
} {
  const eventPolicy = this.options.delivery?.events;
  return {
    offline: eventPolicy?.offline ?? "fail-fast",
    queue: {
      maxSize: eventPolicy?.queue?.maxSize ?? 1000,
      overflow: eventPolicy?.queue?.overflow ?? "reject"
    }
  };
}
```

新增 call queue helper：

```ts
private resolveCallQueueOptions(): Required<ClientDeliveryQueueOptions> {
  const queue = this.options.delivery?.calls?.queue;
  return {
    maxSize: queue?.maxSize ?? 1000,
    overflow: queue?.overflow ?? "reject"
  };
}
```

### Step 3：改 `callRaw()` 使用 resolved options

当前逻辑：

```ts
async callRaw(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
  if (options?.offlinePolicy === "wait-ready") {
    ...
  }
  if (options?.offlinePolicy === "queue") {
    ...
  }
  ...
}
```

改为：

```ts
async callRaw(method: string, params: unknown, options?: CallOptions): Promise<unknown> {
  const resolvedOptions = this.resolveCallOptions(method, options);

  if (resolvedOptions.offlinePolicy === "wait-ready") {
    const ep = await this.waitForUsable();
    return ep.call(method, params, resolvedOptions.timeoutMs);
  }

  if (resolvedOptions.offlinePolicy === "queue") {
    const ep = this.endpoint;
    if (this.state === "ready" && ep !== undefined) {
      return ep.call(method, params, resolvedOptions.timeoutMs);
    }
    return this.enqueueCall(method, params, resolvedOptions);
  }

  const ep = this.requireUsable();
  return ep.call(method, params, resolvedOptions.timeoutMs);
}
```

### Step 4：改 `enqueueCall()` queue 配置来源

改为只读取 `delivery.calls.queue`：

```ts
const queue = this.resolveCallQueueOptions();
const maxSize = queue.maxSize;
const overflow = queue.overflow;
```

### Step 5：改 `enqueueEvent()` policy 来源

改为只读取 `delivery.events`，不再读取旧 `outbox`：

```ts
const policy = this.resolveEventDeliveryPolicy();
if (policy.offline !== "queue") {
  try {
    this.requireUsable();
  } catch (err) {
    return Promise.reject(err);
  }
}
const maxSize = policy.queue.maxSize;
const overflow = policy.queue.overflow;
```

注意：ready 时 `emitRaw()` 仍直接发送，不走 queue。

### Step 6：更新文档

修改：`docs/usage.md`

需要：

1. `ClientOptions` 表格加入 `delivery?: ClientDeliveryOptions`。
2. 将推荐示例从 `outbox` 改为 `delivery.events`。
3. 增加 `delivery.calls.default` / `delivery.calls.methods` 示例。
4. 明确旧 `outbox` / `calls.queue` 已删除，必须迁移到 `delivery`。
5. 明确 RPC 默认仍为 `fail-fast`。

## 10. 测试计划

修改：`tests/sdk/sdk.test.ts`

### 10.1 event delivery 替代 outbox

新增测试：

- `queues client events before ready when delivery.events.offline is queue`
- `delivery.events.queue overflow drop-oldest`
- old `outbox` no longer appears in runtime tests because it is removed from `ClientOptions`

关键断言：

```ts
const client = new AxtpClient(loop.client, {
  logicalRole: "client",
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1, overflow: "drop-oldest" }
    }
  }
});
```

### 10.2 call default policy

新增测试：

- `uses delivery.calls.default for offline wait-ready calls`

关键断言：

```ts
const client = new AxtpClient(loop.client, {
  logicalRole: "client",
  delivery: {
    calls: {
      default: { offlinePolicy: "wait-ready", timeoutMs: 5_000 }
    }
  }
});

const called = client.callRaw("add", { a: 4, b: 6 });
await client.connect();
await expect(called).resolves.toBe(10);
```

### 10.3 method policy 覆盖 default

新增测试：

- `uses delivery.calls.methods to override call default policy`

场景：

```ts
delivery: {
  calls: {
    default: { offlinePolicy: "wait-ready" },
    methods: {
      dangerous: { offlinePolicy: "fail-fast" }
    }
  }
}
```

断言 `client.callRaw("dangerous", {})` 未 ready 时 reject。

### 10.4 per-call 覆盖 method/default

新增测试：

- `per-call options override delivery call policy`

场景：default wait-ready，但调用传 `offlinePolicy: "fail-fast"`，应 reject。

### 10.5 method-level queue/coalesce

新增测试：

- `coalesces queued RPC calls using delivery.calls.methods policy`

复用现有 coalesce 测试，只把 per-call options 移到 client options。

## 11. 验证命令

建议执行：

```bash
pnpm test tests/sdk/sdk.test.ts
pnpm typecheck
pnpm build
```

若项目脚本名称不同，以 `package.json` 为准。

## 12. 风险与权衡

### 风险 1：默认 call policy 可能诱导用户对非幂等 RPC 启用等待/queue

缓解：

- SDK 默认仍 fail-fast。
- 文档强调只有 safe/idempotent method 应配置 wait-ready / queue。
- method-level override 用于保护危险方法。

### 风险 2：breaking change 影响现有调用方

缓解：

- spec 和 usage 文档提供明确迁移示例。
- TypeScript 层删除旧字段，让调用方在升级时尽早发现并迁移。

### 风险 3：通配符 method policy 复杂化

缓解：

- MVP 仅支持精确 method name。
- 通配符另开 spec，不混入第一版。

### 风险 4：`timeoutMs` 不包含 wait-ready / queue 时间

当前文档已有说明：`timeoutMs` applies to the RPC after ready。引入 default policy 后必须继续强调，避免用户误以为它是端到端 deadline。

## 13. 最终建议

采用以下方向：

> 不要把 `outbox` 和 `CallOptions` 粗暴合并成一个扁平对象；而是将 event outbox、call queue、call options 上提到统一的 `delivery` policy 体系。`CallOptions` 继续存在，但定位为单次 call override。

推荐最终 API：

```ts
const client = new AxtpClient(transport, {
  delivery: {
    events: {
      offline: "queue",
      queue: { maxSize: 1000, overflow: "reject" }
    },
    calls: {
      default: {
        offlinePolicy: "wait-ready",
        timeoutMs: 5_000
      },
      queue: { maxSize: 1000, overflow: "reject" },
      methods: {
        "device.factoryReset": {
          offlinePolicy: "fail-fast"
        },
        "cast.setAirPlayName": {
          offlinePolicy: "queue",
          coalesceKey: "cast.setAirPlayName",
          coalescePrevious: "resolve-with-next"
        }
      }
    }
  }
});
```

这样同时解决：

1. 不必每次 RPC 都显式写 offline policy。
2. 通信交付策略不再散落在 `outbox` / `calls.queue` / `CallOptions` 三处。
3. 仍保留 event / RPC / stream 的语义边界，避免把 fire-and-forget 和 request-response 混为一谈。
