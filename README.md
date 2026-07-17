# SeaTunnel DTS Source Connector

阿里云 DTS 订阅数据的 SeaTunnel Source 插件，直接消费 DTS SDK（`DefaultDTSConsumer`），输出带真实库表/列名的 `SeaTunnelRow`，无需 `dts-bridge` TCP 桥接。

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
| DTS SDK | `dts-sdk.jar` 须单独部署，见下文 [dts-sdk.jar 安装](#dts-sdkjar-安装) |
| DTS 凭证 | 参考 `../dts-bridge/config.properties` 或 `config.properties.example` |
| 网络 | 可访问 DTS broker（`:18001`） |

## dts-sdk.jar 安装（**不随仓库分发**）

`*.jar` / `dts-sdk.jar` **禁止 commit**。本仓库只提供文档与源码；请自行下载并放到约定路径。

### 什么是 dts-sdk.jar

- 阿里云 DTS **订阅** Java SDK（`DefaultDTSConsumer` 等），**非本仓库编译产物**
- 本工程编译期通过 `connector-dts/pom.xml` 的 `systemPath` 引用本地 jar（默认 `../dts-bridge/dts-sdk.jar`），运行期由 SeaTunnel 插件目录加载

### 官方获取渠道（推荐）

| 渠道 | 链接 |
|------|------|
| GitHub 源码 / 使用说明 | [aliyun/aliyun-dts-subscribe-sdk-java](https://github.com/aliyun/aliyun-dts-subscribe-sdk-java) |
| Maven Central | [com.aliyun.dts:dts-new-subscribe-sdk](https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk) |
| 阿里云帮助（SDK Demo 消费订阅） | [使用 SDK 客户端消费订阅数据](https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data) |
| 英文文档 | [Use an SDK to consume change tracking data](https://www.alibabacloud.com/help/en/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data) |
| Kafka 客户端消费（对照） | [使用 Kafka 客户端消费订阅数据](https://help.aliyun.com/zh/dts/user-guide/use-a-kafka-client-to-consume-tracked-data-2) |

Maven 依赖示例（公开 SDK；版本以 Maven Central 最新为准）：

```xml
<dependency>
  <groupId>com.aliyun.dts</groupId>
  <artifactId>dts-new-subscribe-sdk</artifactId>
  <version>2.1.4</version>
</dependency>
```

也可从 DTS 控制台订阅任务页下载 Kafka 客户端 demo / 订阅 SDK。

### 本仓库当前用法（本地闭源/专有形态）

开发机通常已有 `../dts-bridge/dts-sdk.jar`（与公开 Maven 坐标形态可能不同，**如实按本地 path 使用**）：

1. 自行下载或从已有环境拷贝 SDK jar
2. 放到约定路径：`../dts-bridge/dts-sdk.jar`（与 `pom.xml` 中 `${dts.sdk.path}` 一致）
3. **不要**把 jar 加入 git；`.gitignore` 已忽略 `*.jar`

### 为何与 connector jar 分离

| 原因 | 说明 |
|------|------|
| 分发 | SDK **不进本仓库**，自行下载放置 |
| SeaTunnel 插件隔离 | SDK 须放在 `plugins/connector-dts/dts-sdk.jar`，由插件 classloader 加载 |
| plugin-mapping | `seatunnel.source.Dts = connector-dts`（子目录名 `connector-dts` 须与 mapping 值一致） |

### 安装步骤

**本地开发（Mac / 本机 SeaTunnel）**

```bash
# 确保 SDK 已放到 ../dts-bridge/dts-sdk.jar（自行下载，勿提交）
cd seatunnel-connector-dts
sh build.sh   # 编译 connector，并拷贝 SDK 到 plugins/connector-dts/
```

**远程 / 新机器**

```bash
# 1. 将自行准备的 dts-sdk.jar 传到目标机
scp /path/to/dts-sdk.jar user@host:/tmp/

# 2. 一键部署（推荐）
cd ..   # seatunnel 工作区根目录
DTS_SDK_JAR=/tmp/dts-sdk.jar sh scripts/deploy-seatunnel-dts.sh

# 或手动放置
mkdir -p /path/to/apache-seatunnel-2.3.13/plugins/connector-dts
cp /tmp/dts-sdk.jar /path/to/apache-seatunnel-2.3.13/plugins/connector-dts/dts-sdk.jar
```

**验证**

```bash
ls -lh plugins/connector-dts/dts-sdk.jar   # 约 17MB，非空
```

须同时存在：`connectors/connector-dts-2.3.13.jar` + `plugins/connector-dts/dts-sdk.jar` + `plugin-mapping.properties` 中的 `seatunnel.source.Dts = connector-dts`。

### 故障排查

| 现象 | 原因与处理 |
|------|-----------|
| `NoClassDefFoundError: com/aliyun/dts/subscribe/clients/ConsumerContext` | 缺少 `plugins/connector-dts/dts-sdk.jar`；按上文安装后重启作业 |
| `build.sh` 报 `missing DTS SDK` | `../dts-bridge/dts-sdk.jar` 不存在；按官方链接自行下载后放到该路径 |
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

使用顶层部署脚本一键下载 SeaTunnel、注册 DTS 插件并生成配置模板（**须自备 `dts-sdk.jar`**，见 [dts-sdk.jar 安装](#dts-sdkjar-安装)）：

```bash
cd ..   # seatunnel 工作区根目录
DTS_SDK_JAR=~/dts-sdk.jar sh scripts/deploy-seatunnel-dts.sh
```

常用环境变量：`INSTALL_DIR`（安装路径）、`SKIP_DOWNLOAD=true`（仅更新插件）、`DTS_SDK_JAR`（SDK 路径，**必填**若不在 `dts-bridge/` 默认位置）、`BUILD_CONNECTOR=no` + `CONNECTOR_JAR`（使用预编译 jar）。详见 `scripts/deploy-seatunnel-dts.sh` 头部注释。

## 构建

```bash
cd seatunnel-connector-dts   # 或本仓库根目录
sh build.sh
```

`build.sh` 执行：

1. `mvn -q package -DskipTests` — 编译 `connector-dts-2.3.13.jar`
2. 拷贝 jar → `apache-seatunnel-2.3.13/connectors/`
3. 拷贝 `dts-sdk.jar` → `plugins/connector-dts/`
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
| `table-list` | 否 | 空（全部表） | 表白名单，`db.table` 格式；支持 HOCON 数组或逗号分隔字符串 |
| `dry-run` | 否 | `false` | 仅计数并 commit，跳过 convert/入队/JSON（测速用；**无法**做 table-list 过滤） |

### table-list 示例

```hocon
# HOCON 数组
table-list = ["mydb.orders", "mydb.users"]

# 或逗号分隔（单字符串会被解析为 list 的一项，推荐用数组）
```

白名单外的 DML 事件会被跳过且不输出，但会 `commit` 以推进位点，避免卡在非目标表上。

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
A: 缺少 `dts-sdk.jar`。见上文 [dts-sdk.jar 安装](#dts-sdkjar-安装) — 仅拷贝 `connector-dts-*.jar` 到远程而不部署 SDK 会触发此错误。

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
A: `dry-run=true` 跳过 `DtsRecordConverter`，只计数并 `commit` 推进位点，不入队、不读表名、不序列化 JSON，适合测 SDK 纯消费吞吐（如 11w+ rps）。要做 `table-list` 过滤须 `dry-run=false`；可配合 `skip-columns-json=true` 降低列 JSON 开销。

## 参考

- [实施计划](../docs/plans/2026-06-25-dts-seatunnel-connector.md)
- [SDK API 笔记（英文）](docs/sdk-api-notes.md)
- [SDK API 笔记（中文）](docs/sdk-api-notes.zh.md)
- [dts-bridge README](../dts-bridge/README.md)（凭证与 checkpoint 参考）
- [aliyun-dts-subscribe-sdk-java](https://github.com/aliyun/aliyun-dts-subscribe-sdk-java)
- [dts-new-subscribe-sdk (Maven)](https://central.sonatype.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk)
- [使用 SDK 客户端消费订阅数据](https://help.aliyun.com/zh/dts/user-guide/use-the-sdk-demo-to-consume-tracked-data)
- [阿里云 DTS Kafka 客户端消费文档](https://help.aliyun.com/zh/dts/user-guide/use-a-kafka-client-to-consume-tracked-data-2)
