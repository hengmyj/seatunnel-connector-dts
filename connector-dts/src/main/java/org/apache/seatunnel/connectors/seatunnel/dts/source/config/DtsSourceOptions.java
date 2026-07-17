package org.apache.seatunnel.connectors.seatunnel.dts.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

/**
 * HOCON {@code source { Dts { ... } }} 配置项定义。
 *
 * <p>插件标识为 {@link #IDENTIFIER}（{@code Dts}），须在 {@code plugin-mapping.properties} 中映射为
 * {@code seatunnel.source.Dts = connector-dts}。
 */
public class DtsSourceOptions {

    /** 与 plugin-mapping / HOCON 插件名一致。 */
    public static final String IDENTIFIER = "Dts";

    public static final Option<String> BROKER_URL =
            Options.key("broker-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("DTS Broker 地址，例如 dts-ap-southeast-1.aliyuncs.com:18001");

    public static final Option<String> TOPIC =
            Options.key("topic").stringType().noDefaultValue().withDescription("DTS 订阅 Topic");

    public static final Option<String> SID =
            Options.key("sid")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("DTS 订阅实例 ID；同一 sid 同一时刻只能有一个消费者");

    public static final Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("DTS 消费组用户名");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("DTS 消费组密码");

    public static final Option<String> CHECKPOINT =
            Options.key("checkpoint")
                    .stringType()
                    .defaultValue("0")
                    .withDescription(
                            "起始位点：Unix 秒，或含 @ 时用 offset@timestamp 形式（HOCON 需加引号）");

    public static final Option<Boolean> FORCE_CHECKPOINT =
            Options.key("force-checkpoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "启动时强制使用配置的 checkpoint（忽略本地/远端已提交位点）；重置前需停进程并删 localCheckpointStore-{sid}");

    public static final Option<Integer> MAX_POLL_RECORDS =
            Options.key("max-poll-records")
                    .intType()
                    .defaultValue(500)
                    .withDescription("传给 Kafka 客户端的 max.poll.records（影响 SDK 一次拉取量）");

    public static final Option<Integer> QUEUE_CAPACITY =
            Options.key("queue-capacity")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("SDK 回调线程与 SourceReader 之间的有界队列容量，满则阻塞形成背压");

    public static final Option<Boolean> DRY_RUN =
            Options.key("dry-run")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "跳过 DtsRecordConverter，仅计数并 commit（测速用；无法做 table-list 过滤）；生产务必 false");

    public static final Option<Boolean> SKIP_COLUMNS_JSON =
            Options.key("skip-columns-json")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "白名单内 DML 的 _columns_json 输出为空 {}；table-list 过滤仍生效（Console 测速用）");

    public static final Option<Integer> POLL_BATCH_SIZE =
            Options.key("poll-batch-size")
                    .intType()
                    .defaultValue(2000)
                    .withDescription("每次 pollNext 最多从队列 drain 的行数，减轻队列积压");

    public static final Option<List<String>> TABLE_LIST =
            Options.key("table-list")
                    .listType()
                    .defaultValue(Collections.emptyList())
                    .withDescription(
                            "表白名单 db.table（小写）；未命中 DML 走 skipAndCommit，不构建 _columns_json；空表示全部表");

    public static final Option<Integer> MAX_COLUMN_JSON_LENGTH =
            Options.key("max-column-json-length")
                    .intType()
                    .defaultValue(262144)
                    .withDescription("_columns_json 单列最大字符数，超出截断；0 表示不截断");

    private DtsSourceOptions() {}
}
