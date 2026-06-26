package org.apache.seatunnel.connectors.seatunnel.dts.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.Collections;
import java.util.List;

public class DtsSourceOptions {

    public static final String IDENTIFIER = "Dts";

    public static final Option<String> BROKER_URL =
            Options.key("broker-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("DTS broker URL, e.g. dts-ap-southeast-1.aliyuncs.com:18001");

    public static final Option<String> TOPIC =
            Options.key("topic").stringType().noDefaultValue().withDescription("DTS subscribe topic");

    public static final Option<String> SID =
            Options.key("sid").stringType().noDefaultValue().withDescription("DTS subscribe instance id");

    public static final Option<String> USER =
            Options.key("user").stringType().noDefaultValue().withDescription("DTS consumer group user");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("DTS consumer group password");

    public static final Option<String> CHECKPOINT =
            Options.key("checkpoint")
                    .stringType()
                    .defaultValue("0")
                    .withDescription("Initial checkpoint: unix seconds or offset@timestamp");

    public static final Option<Boolean> FORCE_CHECKPOINT =
            Options.key("force-checkpoint")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Force use configured checkpoint on startup");

    public static final Option<Integer> MAX_POLL_RECORDS =
            Options.key("max-poll-records").intType().defaultValue(500).withDescription("Kafka max.poll.records");

    public static final Option<Integer> QUEUE_CAPACITY =
            Options.key("queue-capacity")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Blocking queue capacity between SDK callback and reader");

    public static final Option<Boolean> DRY_RUN =
            Options.key("dry-run")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "跳过 DtsRecordConverter，仅计数并 commit（约 11 万 rps 测速；无法做 table-list 过滤）");

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
                    .withDescription("Max rows drained per pollNext call");

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
