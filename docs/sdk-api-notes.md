# DTS SDK API Notes (Phase 0)

Probed from `../dts-bridge/dts-sdk.jar` via `jar tf` + `javap`.

## Record API

| Method | Class | Notes |
|--------|-------|-------|
| `getOperationType()` | `DefaultUserRecord` | INSERT / UPDATE / DELETE / DDL |
| `getBeforeImage()` / `getAfterImage()` | `DefaultUserRecord` | `RowImage` with `Value[]` |
| `getSchema()` | `DefaultUserRecord` | `RecordSchema` |
| `getOffset()` | `DefaultUserRecord` | Kafka offset |
| `getSourceTimestamp()` | `DefaultUserRecord` | Event timestamp (seconds) |
| `commit(String metadata)` | `DefaultUserRecord` | Manual commit after processing |

`RecordListener.consume(DefaultUserRecord)` — callback entry point.

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
