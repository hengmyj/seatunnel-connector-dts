# DTS SDK API Notes (Phase 0)

Compile against Maven `com.aliyun.dts:dts-new-subscribe-sdk:2.1.6`. Runtime: `mvn package` copies `jar-with-dependencies` to `connector-dts/target/dts-sdk.jar` → `plugins/connector-dts/dts-sdk.jar`.

- GitHub: https://github.com/aliyun/aliyun-dts-subscribe-sdk-java
- Maven: https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk
- Docs: https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data

`UserRecordGenerator` builds `LazyParseRecordImpl` (header vs payload). Do **not** rewrite RecordGenerator.

## Record API

| Method | Class | Notes |
|--------|-------|-------|
| `getOperationType()` | `UserRecord` / `LazyParseRecordImpl` | Header only. INSERT / UPDATE / DELETE / DDL / HEARTBEAT |
| `getSchema(false)` | `LazyParseRecordImpl` | Header schema; empty schema + `getSchema()` **would** `initPayload` |
| `getDatabaseName()` / `getTableName()` | `LazyRecordSchema` | From `objectName` / tags `l_db_name` `l_tb_name`; does **not** init payload |
| `getFields()` / `getFieldCount()` / `toString()` | `LazyRecordSchema` | **Does** `initPayload` |
| `getBeforeImage()` / `getAfterImage()` | `UserRecord` | **Does** `initPayload` (Avro fields + images) |
| `offset()` | `LazyParseRecordImpl` | Kafka offset (constructor; not on `UserRecord`) |
| `getSourceTimestamp()` | `UserRecord` | Header timestamp |
| `commit(String metadata)` | `UserRecord` | Manual commit after processing |

`RecordListener.consume(UserRecord)` — callback entry point (`LazyParseRecordImpl` at runtime).

## RecordSchema

| Method | Returns |
|--------|---------|
| `getDatabaseName()` | `NullableOptional<String>` |
| `getTableName()` | `NullableOptional<String>` |
| `getFieldNames()` | `List<String>` |
| `getField(int)` / `getField(String)` | `RecordField` |
| `getFieldCount()` | `int` |

`RecordField.getFieldName()` — real column name (not c0/c1).

## ConsumerContext

Constructor (bridge pattern):

```java
new ConsumerContext(null, brokerUrl, topic, sid, user, password, checkpoint,
    ConsumerSubscribeMode.ASSIGN, kafkaProps);
```

| Method | Purpose |
|--------|---------|
| `setForceUseCheckpoint(boolean)` | Ignore local store when true |
| `setUserRegisteredStore(MetaStore<Checkpoint>)` | Phase 3 custom store |
| `getKafkaProperties()` | Pass `max.poll.records` |

## DefaultDTSConsumer

- Wraps `RecordGenerator` / `EtlRecordProcessor` / `WorkThread` internally — **do not reimplement**.
- `addRecordListeners(Map<String, RecordListener>)` + `start()` + `close()`.

## Checkpoint format

- Pure unix seconds: `1769589978`
- Offset@timestamp: `15487026700@1769589978` (parsed by SDK `Util.parseCheckpoint`)
- Local store: `localCheckpointStore-{sid}` in CWD

## Compile dependencies (SeaTunnel 2.3.13)

Provided at build time from `apache-seatunnel-2.3.13/`:

| Jar | Purpose |
|-----|---------|
| `starter/seatunnel-starter.jar` | `seatunnel-api` |
| `connectors/connector-fake-2.3.13.jar` | `connector-common` (`AbstractSingleSplitSource`) |

Runtime: `connectors/connector-dts-2.3.13.jar` + `plugins/connector-dts/dts-sdk.jar`.
