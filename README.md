# SeaTunnel DTS Source Connector

阿里云 DTS 订阅数据的 SeaTunnel Source 插件，直接消费 DTS SDK（`DefaultDTSConsumer`），输出带真实库表/列名的 `SeaTunnelRow`，无需 `dts-bridge` TCP 桥接。

## 为什么需要（实际意义）

企业要追求**稳定**：生产库不宜被下游直连、更不能被业务回放直接打满。

若下游直接对生产做 CDC / 数据回放，会把查询与变更压力压回生产，风险高、成本隐性放大。中间走 **DTS 订阅**，用较小代价把消费与生产隔离——下游按需做**回放**、**多账号消费** 等，而不必碰生产库。

本 connector 的价值就在这条链路上：把 DTS 订阅接进 SeaTunnel，低成本隔离生产，服务回放与多消费场景。

## 架构

```
┌─────────────┐   DTS SDK (Avro)   ┌──────────────────────────┐   SeaTunnelRow   ┌─────────────┐
│ 阿里云 DTS   │ ─────────────────► │ seatunnel-connector-dts  │ ───────────────► │ Console /   │
│ 订阅通道     │   SASL_SSL/ASSIGN  │ (SeaTunnel Source 插件)   │                  │ Jdbc Sink   │
└─────────────┘                    └──────────────────────────┘                  └─────────────┘
```

与 `dts-bridge` 对比：

| 维度 | dts-bridge | connector-dts |
|------|-----------|---------------|
| 集成 | 独立进程 + Socket Source | SeaTunnel 内置 Source |
| 列名 | `c0, c1, ...` | SDK `fieldName` |
| 位点提交 | 写出后 `record.commit()` | 入队成功后 `record.commit()` |
| 运维 | 双进程 | 单进程 |

## 前置条件

| 项 | 要求 |
|----|------|
| JDK | 8 或 11 |
| Maven | 3.6+ |
| SeaTunnel | `../apache-seatunnel-2.3.13/` |
| DTS SDK | Maven `com.aliyun.dts:dts-new-subscribe-sdk`（`2.1.6`，含 `LazyParseRecordImpl`） |
| DTS 凭证 | 参考 `../dts-bridge/config.properties` 或 `config.properties.example` |
| 网络 | 可访问 DTS broker（`:18001`） |

## DTS SDK（Maven 开源）

编译依赖公开坐标 `com.aliyun.dts:dts-new-subscribe-sdk:2.1.6`（[Maven Central](https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk)，Apache-2.0），**不再**引用本地 `../dts-bridge/dts-sdk.jar`。该版本的 `UserRecordGenerator` 产出 `LazyParseRecordImpl`：先反序列化 header（`objectName` → 库表），`getAfterImage()` / `getFields()` 才解码 Avro payload（fields + before/after images）。

```xml
<dependency>
  <groupId>com.aliyun.dts</groupId>
  <artifactId>dts-new-subscribe-sdk</artifactId>
  <version>2.1.6</version>
</dependency>
```

运行期仍由 SeaTunnel 插件 classloader 从 `plugins/connector-dts/` 加载 SDK。`mvn package` 会把 `jar-with-dependencies` 拷到 `connector-dts/target/dts-sdk.jar`，`build.sh` 再部署为 `plugins/connector-dts/dts-sdk.jar`（connector 插件 jar **不含** SDK 类）。

官方渠道：[GitHub](https://github.com/aliyun/aliyun-dts-subscribe-sdk-java) · [使用 SDK 消费订阅数据](https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data)

### 安装步骤

**本地开发**

```bash
cd seatunnel-connector-dts
sh build.sh   # mvn package 拉取 SDK，并拷贝到 plugins/connector-dts/dts-sdk.jar
```

**远程 / 新机器**

```bash
# 1. 构建（或拷贝已构建的 connector jar + target/dts-sdk.jar）
cd seatunnel-connector-dts
mvn -q package -DskipTests

# 2. 部署
mkdir -p /path/to/apache-seatunnel-2.3.13/plugins/connector-dts
cp connector-dts/target/connector-dts-2.3.13.jar /path/to/apache-seatunnel-2.3.13/connectors/
cp connector-dts/target/dts-sdk.jar /path/to/apache-seatunnel-2.3.13/plugins/connector-dts/dts-sdk.jar
```

须同时存在：`connectors/connector-dts-2.3.13.jar` + `plugins/connector-dts/dts-sdk.jar` + `plugin-mapping.properties` 中的 `seatunnel.source.Dts = connector-dts`。

### 故障排查

| 现象 | 原因与处理 |
|------|-----------|
| `NoClassDefFoundError: com/aliyun/dts/subscribe/clients/ConsumerContext` | 缺少 `plugins/connector-dts/dts-sdk.jar`；先 `sh build.sh` 后重启作业 |
| `build.sh` 报 `missing DTS SDK fat jar` | `mvn package` 未拷到 `connector-dts/target/dts-sdk.jar`；检查能否访问 Maven Central |
| 远程仅拷贝了 `connector-dts-*.jar` | connector jar 不含 SDK 类，必须单独部署 `dts-sdk.jar` |

## 项目结构

```
seatunnel-connector-dts/          # 顶层独立 Maven 工程（本地 git 仓库）
├── pom.xml                       # 父 POM，seatunnel.version=2.3.13
├── build.sh                      # mvn package + 部署到 SeaTunnel
├── README.md
├── connector-dts/                # Maven 子模块
│   ├── pom.xml
│   └── src/main/java/.../dts/source/
├── config/
│   ├── dts-to-console.conf.example   # 配置模板（可提交）
│   └── dts-to-console.conf           # 本地凭证（gitignore，勿提交）
└── docs/
    ├── sdk-api-notes.md      # SDK API 笔记（英文）
    └── sdk-api-notes.zh.md   # SDK API 笔记（中文）
```

部署目标（`build.sh` 自动写入）：

```
apache-seatunnel-2.3.13/
├── connectors/
│   ├── connector-dts-2.3.13.jar
│   └── plugin-mapping.properties    # seatunnel.source.Dts = connector-dts
├── config/
│   └── plugin_config                # --connectors-v2-- 段含 connector-dts
└── plugins/
    └── connector-dts/
        └── dts-sdk.jar              # SDK 隔离加载（许可禁止打入 fat jar）
```

## 远程 / 新机器部署

使用顶层部署脚本一键下载 SeaTunnel、注册 DTS 插件并生成配置模板（SDK 由 `mvn package` 从 Maven Central 拉取）：

```bash
cd ..   # seatunnel 工作区根目录
DTS_SDK_JAR=~/dts-sdk.jar sh scripts/deploy-seatunnel-dts.sh
```

常用环境变量：`INSTALL_DIR`（安装路径）、`SKIP_DOWNLOAD=true`（仅更新插件）、`DTS_SDK_JAR`（可选覆盖 SDK 路径）、`BUILD_CONNECTOR=no` + `CONNECTOR_JAR`（使用预编译 jar）。详见 `scripts/deploy-seatunnel-dts.sh` 头部注释。

## 构建

```bash
cd seatunnel-connector-dts   # 或本仓库根目录
sh build.sh
```

`build.sh` 执行：

1. `mvn -q package -DskipTests` — 编译 `connector-dts-2.3.13.jar`
2. 拷贝 jar → `apache-seatunnel-2.3.13/connectors/`
3. 拷贝 Maven 拉取的 `dts-sdk.jar` → `plugins/connector-dts/`
4. 注册 `plugin-mapping.properties`（若尚未存在）
5. 在 `plugin_config` 的 `--connectors-v2--` 段启用 `connector-dts`

## 配置

### 从模板创建

```bash
cp config/dts-to-console.conf.example config/dts-to-console.conf
# 编辑 broker-url / topic / sid / user / password
```

### 从 dts-bridge 迁移

`dts-bridge/config.properties` 与 HOCON 配置对应关系：

| dts-bridge | connector-dts HOCON |
|------------|---------------------|
| `brokerUrl` | `broker-url` |
| `topic` | `topic` |
| `sid` | `sid` |
| `user` | `user` |
| `password` | `password` |
| `checkpoint` | `checkpoint` |
| `forceCheckpoint` | `force-checkpoint` |
| `maxPollRecords` | `max-poll-records` |
| `dryRun` | `dry-run` |

### 配置项说明

| 配置项 | 必填 | 默认值 | 说明 |
|--------|------|--------|------|
| `broker-url` | 是 | — | DTS broker 地址，如 `dts-ap-southeast-1.aliyuncs.com:18001` |
| `topic` | 是 | — | DTS 订阅 topic |
| `sid` | 是 | — | 订阅实例 ID |
| `user` | 是 | — | 消费组账号 |
| `password` | 是 | — | 消费组密码 |
| `checkpoint` | 否 | `0` | 起始位点：Unix 秒级时间戳，或 `offset@timestamp` |
| `force-checkpoint` | 否 | `false` | `true` 时每次启动强制使用 `checkpoint`，忽略本地 store |
| `max-poll-records` | 否 | `500` | Kafka `max.poll.records`，调大可提高吞吐 |
| `queue-capacity` | 否 | `10000` | SDK 回调与 Reader 之间有界队列容量 |
| `table-list` | 否 | 空（全部表） | 表白名单，`db.table` 格式；未命中只读 header 库表后 skip+commit，不解码 Avro 行镜像 |
| `dry-run` | 否 | `false` | 仅计数并 commit，跳过 convert/入队/JSON（测速用；**无法**做 table-list 过滤） |
| `skip-columns-json` | 否 | `false` | 命中表仍输出信封，但 `_columns_json={}` 且不调用 `getAfterImage()` |

### table-list 示例

```hocon
# HOCON 数组
table-list = ["mydb.orders", "mydb.users"]

# 或逗号分隔（单字符串会被解析为 list 的一项，推荐用数组）
```

白名单外的 DML 只从 header 读取库表名后 `commit`，**不解码** `beforeImages`/`afterImages`。在 DTS 控制台收窄订阅对象仍可节省带宽（客户端 skip 无法减少 broker 推送）。

## 性能 / 吞吐参考

> 以下数字来自本地/联调历史与用户测速反馈，**不是**正式压测报告；环境、订阅内容、下游 Sink 不同时结果会差一个数量级。

### 消费速度取决于

| 因素 | 说明（有历史验证） |
|------|-------------------|
| **`dry-run`** | `true`：跳过 convert / 入队 / `_columns_json`，只计数并 `commit`，测 SDK 纯消费上限；`false` 才走表名过滤与真实输出 |
| **是否构建 `_columns_json`** | 宽表 / 大字段序列化是 DML 主开销；`skip-columns-json=true` 可测「不过列 JSON、不解码行镜像」路径（测速用） |
| **`table-list` 过滤比例** | 白名单外只读 header 库表后 `skipAndCommit`，不解码 Avro images；过滤越多，CPU 越省 |
| **下游 Sink** | Console 打印大 JSON 会严重拖慢；真实 Jdbc 等 Sink 才能代表业务吞吐 |
| **JVM 堆** | 默认 `jvm_client_options` 约 `-Xmx512m` 易 OOM；生产建议 `-Xmx4g`（或等价 `JvmOption`） |
| **队列与背压** | connector `queue-capacity`（默认 10000）满则 SDK 回调阻塞；SDK 内部队列上限约 **512**，下游慢 / DDL 段 `skip` 不 `commit` 时易顶满 |
| **同一 `sid`** | 同一消费组同时只能一个消费者；本地与生产抢同一 `sid` 会互相拖慢或抢位点 |
| **机器规格 / 网络** | CPU、堆、到 DTS broker 的网络都会影响峰值；订阅内心跳/非 DML 比例也会让 `inRps` 与有效行数差很多 |

### 指标怎么读（勿混用）

| 指标 | 含义 | 能否当「下游吞吐」 |
|------|------|-------------------|
| SDK **`inRps`** | Kafka 拉取进入 SDK 管道的速率（含心跳等） | 否 |
| SDK **`outRps`** | 调用了 `record.commit()` 的速率（含白名单外 skipAndCommit） | 否（接近「位点推进」而非 Sink 写出） |
| 日志 **`DTS committed N`** / SeaTunnel Read Count | Source 成功处理并交给引擎的行数 | 接近 Source 侧有效吞吐 |
| Sink 写出 / 目标库入库 | 真实下游 | **是**（生产应以这项为准） |

高 `inRps`、低 `outRps` 在非 DML 占比高时属正常；Console + 全量 `_columns_json` 时 `outRps` 只有百级也常见，不代表「SDK 只拉了几百条」。

### 历史测速摘要

| 场景 | 量级 | 条件 / 备注 |
|------|------|-------------|
| connector **`dry-run=true`** | 约 **11 万+** rps（用户测速）；控制台峰值约 **109k** rps | 跳过 convert/JSON/入队，仍 `commit`；测 SDK 消费上限，**无表过滤、无真实下游行** |
| dts-bridge 空跑优化后 | SDK `inRps` 约 **7–8 万** | 跳过无客户端 JSON；与 connector dry-run 同属「轻处理」上限参考 |
| connector 正式路径 + Console | `inRps` 约 **1.5–1.6 万**，`outRps` 约 **百级** | `dry-run=false` + 全量 `_columns_json`；多数非 DML 不 commit |
| 默认堆 / 无白名单 | 易 OOM、队列顶满后位点停 | `-Xmx512m` + 大字段 JSON；SDK 线程挂掉后引擎仍可能继续 checkpoint |

### 参考配置结论（用户口径）

- **约 4C 规格下，消费可达 10W+ 下游**（规划/容量口径）。
- **已验证的同量级证据**：`dry-run=true` 测速约 **11w+** rps（含控制台峰值 ~109k）。该数字是 **SDK 轻路径 commit 上限**，不是「Jdbc 全量列同步已稳定 10W+」的压测结论。
- 生产要接近该量级，需同时满足：足够 CPU（约 4C 量级）、`-Xmx4g` 级堆、合理 `table-list` / 控制台缩订阅、避免与生产抢同一 `sid`、下游 Sink 跟得上；并关掉测速开关（`dry-run=false`，生产勿长期 `skip-columns-json=true`）。

测速建议：

```hocon
# 纯 SDK 上限（无表过滤、无下游有效行）
dry-run = true

# 要 table-list + 仍测转换路径（不做列 JSON）
dry-run = false
skip-columns-json = true
table-list = ["mydb.target_table"]
```

## 运行

```bash
cd ../apache-seatunnel-2.3.13
./bin/seatunnel.sh --config ../seatunnel-connector-dts/config/dts-to-console.conf -m local
```

最小作业配置：

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 10000
  parallelism = 1
}

source {
  Dts {
    broker-url = "dts-ap-southeast-1.aliyuncs.com:18001"
    topic = "your_topic"
    sid = "your_sid"
    user = "your_user"
    password = "your_password"
    checkpoint = "0"
    force-checkpoint = false
    table-list = ["mydb.target_table"]
  }
}

sink {
  Console {}
}
```

## 输出 Schema（Phase 1/2）

| 字段 | 类型 | 说明 |
|------|------|------|
| `_database` | string | 库名 |
| `_table` | string | 表名 |
| `_op` | string | INSERT / UPDATE / DELETE |
| `_ts` | long | 源库时间戳（秒） |
| `_offset` | long | DTS offset |
| `_columns_json` | string | 列名→值的 JSON（after 镜像） |

## Checkpoint 与位点

### 双层存储

| 层级 | 位置 | 说明 |
|------|------|------|
| DTS SDK 本地 store | 进程 CWD 下 `localCheckpointStore-{sid}/` | SDK 自动持久化已 commit 的位点 |
| SeaTunnel checkpoint | Zeta 引擎 checkpoint 目录 | 作业级状态（Phase 3 将与 SDK store 对齐） |

### 重置位点 SOP

1. 停止 SeaTunnel 作业
2. 修改 `checkpoint` 为目标时间戳（须在 DTS 数据保留范围内）
3. 设置 `force-checkpoint = true`
4. 删除本地 SDK store：`rm -rf localCheckpointStore-{sid}`（在作业工作目录下，通常是 `apache-seatunnel-2.3.13/` 或启动目录）
5. 重启作业
6. 在日志中确认 `Seeking` / seek offset，**不要**仅依赖 DTS 控制台消费组位置（控制台显示的是最后 commit 的 offset，不是 seek 目标）

### checkpoint 格式

- 纯时间戳：`1782181302`
- offset@timestamp：`15487026700@1782181302`（SDK `Util.parseCheckpoint` 解析）

## Git 说明

- 本目录为**本地专用** git 仓库，默认不推送远程
- **勿提交**：`*.jar`（含 `dts-sdk.jar`）、真实 `config/*.conf`、`**/config.properties`、`.env`、密钥文件、`script/`（可能含 OSS AK）
- **可提交**：`config/*.conf.example`、源码、文档；示例里用 `your_password` 等占位符
- `localCheckpointStore-*`、`target/`、`*.log` 已 gitignore

## 常见问题

**Q: 运行时报 `NoClassDefFoundError: com/aliyun/dts/subscribe/clients/ConsumerContext`？**  
A: 缺少 `dts-sdk.jar`。见上文 [DTS SDK（Maven 开源）](#dts-sdkmaven-开源) — 仅拷贝 `connector-dts-*.jar` 到远程而不部署 SDK 会触发此错误。

**Q: 启动报找不到 Dts 插件？**  
A: 先执行 `sh build.sh`（或远程 `deploy-seatunnel-dts.sh`），确认 `connectors/connector-dts-2.3.13.jar`、`plugins/connector-dts/dts-sdk.jar` 均存在，且 `plugin-mapping.properties` 含 `seatunnel.source.Dts = connector-dts`。

**Q: 认证失败 / CheckResult{isOk=false}？**  
A: 检查 `user`/`password`/`sid`/`topic`；DTS SASL 用户名格式为 `{user}-{consumer.group}`。

**Q: 有消费速度但 Console 无输出？**  
A: 检查 `table-list` 是否过滤了目标表；日志中 `filtered_tables=N` 持续增长说明白名单外事件被跳过。

**Q: 位点重置后仍从旧位置消费？**  
A: 确认 `force-checkpoint=true`、已删除 `localCheckpointStore-{sid}`、作业已完全停止后重启。

**Q: 队列阻塞 / 消费变慢？**  
A: 调大 `queue-capacity` 或 `max-poll-records`；确认下游 Sink 无背压。

**Q: dry-run 与正式运行的区别？**  
A: `dry-run=true` 跳过 `DtsRecordConverter`，只计数并 `commit` 推进位点，不入队、不读表名、不序列化 JSON，适合测 SDK 纯消费吞吐（如 11w+ rps）。要做 `table-list` 过滤须 `dry-run=false`；可配合 `skip-columns-json=true`（不解码行镜像）。

## 参考

- [实施计划](../docs/plans/2026-06-25-dts-seatunnel-connector.md)
- [SDK API 笔记（英文）](docs/sdk-api-notes.md)
- [SDK API 笔记（中文）](docs/sdk-api-notes.zh.md)
- [dts-bridge README](../dts-bridge/README.md)（凭证与 checkpoint 参考）
- [aliyun-dts-subscribe-sdk-java](https://github.com/aliyun/aliyun-dts-subscribe-sdk-java)
- [dts-new-subscribe-sdk (Maven)](https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk)
- [使用 SDK 客户端消费订阅数据](https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data)
- [阿里云 DTS Kafka 客户端消费文档](https://help.aliyun.com/zh/dts/user-guide/use-a-kafka-client-to-consume-tracked-data-2)
