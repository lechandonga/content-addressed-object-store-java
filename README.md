# 内容寻址本地对象存储（Content-Addressed Object Store）

纯本地磁盘实现的版本化对象存储，无外部服务或中间件依赖（仅需一个可配置的数据目录）。
基于 Spring Boot 3 / Java 21，核心引擎位于
`com.github.lechandonga.contentaddressedobjectstorejava.store.engine`，
不依赖 Spring，可作为库直接嵌入使用。

## 能力概览

- **内容去重**：上传内容按 SHA-256 寻址。相同字节无论重复上传还是跨对象复用，
  `blobs/` 中只保留一份数据（通过硬链接发布，零拷贝）。
- **完整性校验**：读取已提交版本时先把字节经临时文件全量重算 SHA-256 并比对长度，
  校验通过才把字节交给调用方；校验失败抛出可区分的
  `ContentCorruptedException`，调用方输出流不会收到任何错误数据。
  已提交 blob 丢失同样按损坏报告（`ObjectNotFoundException` 仅用于对象/版本不存在）。
- **版本管理**：同一对象每次上传产生一个不可变版本，版本号从 1 开始、严格递增、
  无空洞，元数据记录版本号、内容哈希、字节长度和提交时间。
- **并发写入**：提交是"读元数据 → 校验期望版本 → 发布 blob → 原子替换元数据"的
  CAS 序列，由 JVM 内锁 + 每对象文件锁（跨进程）串行化。条件更新携带
  `expectedVersion`：`0` 表示仅当对象不存在，正数表示基于该版本更新，`-1` 表示无条件追加。
  并发竞争时只有一方成功，其余方收到 `VersionConflictException`（HTTP 409），
  不会静默覆盖或丢失已提交版本。
- **安全回收**：GC 只回收不被任何已提交版本引用的 blob；宽限期年龄阈值保护
  正在写入的数据；两阶段（移入 `quarantine/` 后删除）+ 持锁二次扫描营救，
  保证与提交并发执行时也不会误删正在提交的 blob。
- **崩溃恢复**：所有写入都走 `tmp/` 暂存 + fsync + 原子重命名；
  中途崩溃只会留下临时碎片或隔离区残留。重启时自动营救隔离区中仍被引用的 blob、
  清理过期暂存碎片；已提交版本始终完整可读，未提交内容不会以半成品暴露。

## 磁盘布局

```
<data-dir>/
├── blobs/                     # 内容寻址、不可变的数据文件
│   └── ab/cd/<sha256>         # 两级分片，文件名为内容完整 SHA-256（hex）
├── objects/                   # 对象元数据（版本历史）
│   └── 12/34/<sha256(objectId)>.json
├── locks/                     # 提交锁：<sha256(objectId)>.lock 与 gc.lock
├── tmp/                       # 所有暂存：blob-*.part / meta-*.json.tmp / read-*.part
└── quarantine/                # GC 两阶段回收的隔离区（删除前的暂存位置）
```

- blob 文件一旦发布即不可变；发现同名 blob 已存在时会先校验其字节再决定复用。
- 元数据 JSON 结构示例：

```json
{
  "objectId": "report-2026",
  "latestVersion": 2,
  "versions": [
    { "version": 1, "contentHash": "ab12…", "size": 128, "committedAt": "2026-10-05T11:00:00Z" },
    { "version": 2, "contentHash": "cd34…", "size": 256, "committedAt": "2026-10-05T11:05:00Z" }
  ]
}
```

元数据文件以"写临时文件 → fsync → 原子 rename 覆盖"更新，读者只会看到完整的
旧版本或完整的新版本。

## 提交与一致性规则

1. 上传字节流式写入 `tmp/blob-*.part`，同时计算 SHA-256，写完 fsync。
2. 在对象锁内读取当前元数据并比对 `expectedVersion`，不符立即失败并清理暂存。
3. 用硬链接把暂存内容发布为 `blobs/<hash>`；已存在则先校验再复用（去重）。
4. 新元数据写入 `tmp/meta-*.json.tmp`（fsync），原子 rename 覆盖旧元数据。
5. 清理暂存链接（只是 `tmp` 下的目录项，不影响已发布 blob 的 inode）。

崩溃后果只有三种，且都可恢复：提交前崩溃（仅有 tmp 碎片）、元数据替换前后崩溃
（旧/新元数据均完整）、GC 删除前崩溃（blob 停留在 quarantine，重启按引用集营救或清理）。

## 垃圾回收规则

GC 由全局 JVM 锁与 `locks/gc.lock`（跨进程）串行执行：

1. 清理 `tmp/` 中超过 `temp-sweep-min-age` 的暂存碎片。
2. 扫描全部元数据得到引用哈希集合。
3. 对未被引用且修改时间早于 `now - gc-grace-period` 的 blob，原子移入 `quarantine/`；
   年轻 blob 视为可能属于在途写入，本轮跳过。
4. **持每个对象的提交锁重新扫描引用集并营救隔离区中被引用的 blob**，再确认一轮。
   由于提交顺序是"先发布 blob，后替换元数据"，且锁让扫描与提交互不交叠，
   任何被已提交版本引用的 blob 都不可能被最终删除。
5. 删除隔离区中剩余 blob，返回扫描/存活/回收/清理/营救计数。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `PUT` | `/objects/{id}` | 上传（`Content-Type: application/octet-stream`）；条件提交用 `If-Match: <expectedVersion>` |
| `GET` | `/objects/{id}` | 读取最新版本；`X-Version: n` 读取指定历史版本 |
| `GET` | `/objects/{id}/versions` | 版本历史（JSON） |
| `POST` | `/objects/gc/run` | 执行一次垃圾回收，返回统计报告 |

错误状态码：`409` 版本冲突、`422` 内容校验失败（损坏）、`404` 对象或版本不存在。

## 可配置项

| 属性 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `objectstore.data-dir` | `OBJECT_STORE_HOME` | `./data/object-store` | 数据根目录（需同盘支持硬链接） |
| `objectstore.gc-grace-period` | — | `10m` | blob 可被回收的最小年龄，保护在途写入 |
| `objectstore.temp-sweep-min-age` | — | `10m` | 启动时允许清理的暂存文件最小年龄 |
| `server.port` | `SERVER_PORT` | `0`（随机端口） | HTTP 端口 |

作为库使用时直接构造：

```java
ObjectStore store = new LocalObjectStore(new StoreConfig(Path.of("/var/lib/objstore")));
PutResult r = store.put("obj-1", inputStream, 0);
store.get("obj-1", r.version(), outputStream);
GarbageCollectionReport report = store.collectGarbage();
```

## 测试

```bash
mvn test
```

测试按场景组织（`LocalObjectStoreTest`），每个用例通过 SLF4J 打印
`CASE x.y START/PASS`，覆盖：

1. **去重与版本**（1.1）：重复上传、跨对象复用只占一份 blob；版本号连续、历史字节一致。
2. **完整性**（2.1/2.2）：篡改/删除已提交 blob 时返回 `ContentCorruptedException`
   且调用方拿不到任何字节；对象不存在与内容损坏错误可区分。
3. **并发冲突**（3.1/3.2）：8 个写方同条件竞争仅 1 个成功、7 个显式冲突；
   过期 `expectedVersion` 被拒绝。
4. **中断恢复**（4.1–4.3）：暂存碎片重启后清理且不暴露为版本；GC 中断在隔离区时
   仍引用的 blob 被营救；重启后全部历史版本可读。
5. **安全回收**（5.1/5.2）：仅回收未引用 blob，历史版本引用保留；
   宽限期内的年轻数据不被回收。
