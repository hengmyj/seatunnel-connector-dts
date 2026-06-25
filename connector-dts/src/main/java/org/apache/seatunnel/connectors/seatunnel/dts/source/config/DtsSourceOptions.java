package org.apache.seatunnel.connectors.seatunnel.dts.source.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

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
                    .withDescription("Count records only, skip enqueue and commit");

    private DtsSourceOptions() {}
}
