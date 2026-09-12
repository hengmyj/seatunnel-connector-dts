# DTS SDK API 笔记（Phase 0）

业务动机与方案价值见 [README · 为什么需要（实际意义）](../README.md#为什么需要实际意义)。

基于 Maven `com.aliyun.dts:dts-new-subscribe-sdk:2.1.6`（含 `LazyParseRecordImpl` header/payload 分离）。

## SDK 来源与放置

编译走 Maven Central；`mvn package` 把 `jar-with-dependencies` 写成 `connector-dts/target/dts-sdk.jar`，`build.sh` 部署到 `plugins/connector-dts/dts-sdk.jar`。**勿把 jar 或真实密钥 commit 进 git**。

官方获取渠道：

| 渠道 | 链接 |
|------|------|
| GitHub | https://github.com/aliyun/aliyun-dts-subscribe-sdk-java |
| Maven Central | https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk |
| 阿里云帮助文档 | https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data |

- 编译期：`com.aliyun.dts:dts-new-subscribe-sdk:2.1.6`
- 运行期路径：`plugins/connector-dts/dts-sdk.jar`（由 `build.sh` 拷贝 fat jar）
- 凭证占位：`your_user` / `your_password`；真实配置见 `config/dts-to-console.conf`（gitignore）

## 与 connector 的关系

| 组件 | 职责 |
|------|------|
| `dts-sdk.jar` | Maven `dts-new-subscribe-sdk` fat jar（`DefaultDTSConsumer`、`LazyParseRecordImpl` 等） |
| `connector-dts-*.jar` | SeaTunnel Source 插件，封装 SDK 回调并输出 `SeaTunnelRow` |
| `dts-bridge` | 可选参考：独立 TCP 桥；本 connector **直接消费 SDK**，无需桥接 |

要点：

- connector jar **不含** SDK 类；远程部署须同时放置两者
- SDK 由插件 classloader 从 `plugins/connector-dts/` 隔离加载（许可禁止打入 fat jar）
- 插件映射：`seatunnel.source.Dts = connector-dts`

## Record API

| 方法 | 类 | 说明 |
|------|-----|------|
| `getOperationType()` | `UserRecord` / `LazyParseRecordImpl` | header：INSERT / UPDATE / DELETE / DDL / HEARTBEAT |
| `getSchema(false)` | `LazyParseRecordImpl` | header schema；空 schema 时 `getSchema()` 会 `initPayload` |
| `getDatabaseName()` / `getTableName()` | `LazyRecordSchema` | 来自 `objectName` / tags `l_db_name` `l_tb_name`，**不**触发 payload |
| `getFields()` / `getFieldCount()` / `toString()` | `LazyRecordSchema` | **会** `initPayload` |
| `getBeforeImage()` / `getAfterImage()` | `UserRecord` | **会**解码 Avro fields + images |
| `offset()` | `LazyParseRecordImpl` | Kafka offset（构造即有；不在 `UserRecord` 接口上） |
| `getSourceTimestamp()` | `UserRecord` | 事件时间戳（秒） |
| `commit(String metadata)` | `UserRecord` | 处理成功后手动提交位点 |

入口回调：`RecordListener.consume(UserRecord)`（运行时为 `LazyParseRecordImpl`）。

## RecordSchema

| 方法 | 返回值 |
|------|--------|
| `getDatabaseName()` | `NullableOptional<String>` |
| `getTableName()` | `NullableOptional<String>` |
| `getFieldNames()` | `List<String>` |
| `getField(int)` / `getField(String)` | `RecordField` |
| `getFieldCount()` | `int` |

`RecordField.getFieldName()` — 真实列名（非 bridge 的 `c0`/`c1`）。

## ConsumerContext

构造方式（与 bridge 一致）：

```java
new ConsumerContext(null, brokerUrl, topic, sid, user, password, checkpoint,
    ConsumerSubscribeMode.ASSIGN, kafkaProps);
```

| 方法 | 用途 |
|------|------|
| `setForceUseCheckpoint(boolean)` | `true` 时忽略本地 store，强制使用配置位点 |
| `setUserRegisteredStore(MetaStore<Checkpoint>)` | Phase 3 自定义 store |
| `getKafkaProperties()` | 传入 `max.poll.records` 等 Kafka 参数 |

凭证示例（占位符，勿提交真实值）：

```text
broker-url = "dts-xxx.aliyuncs.com:18001"
topic = "your_topic"
sid = "your_sid"
user = "your_user"
password = "your_password"
```

## DefaultDTSConsumer

- 内部封装 `RecordGenerator` / `EtlRecordProcessor` / `WorkThread` — **不要自行重写**
- 用法：`addRecordListeners(Map<String, RecordListener>)` → `start()` → `close()`

## Checkpoint 格式

| 格式 | 示例 | 说明 |
|------|------|------|
| 纯 Unix 秒 | `1769589978` | 起始时间戳 |
| offset@timestamp | `15487026700@1769589978` | 由 SDK `Util.parseCheckpoint` 解析 |
| 本地 store | `localCheckpointStore-{sid}` | 位于进程 CWD，勿提交 |

## 编译依赖（SeaTunnel 2.3.13）

构建时从 `../apache-seatunnel-2.3.13/` 提供：

| Jar | 用途 |
|-----|------|
| `starter/seatunnel-starter.jar` | `seatunnel-api` |
| `connectors/connector-fake-2.3.13.jar` | `connector-common`（`AbstractSingleSplitSource`） |

运行时须同时存在：

- `connectors/connector-dts-2.3.13.jar`
- `plugins/connector-dts/dts-sdk.jar`

## 相关文档

- 英文版笔记：[sdk-api-notes.md](sdk-api-notes.md)
- 项目 README：[../README.md](../README.md)（安装、配置、运行）
