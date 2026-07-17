package org.apache.seatunnel.connectors.seatunnel.dts.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 从 {@link ReadonlyConfig} 解析出的不可变运行时配置。
 *
 * <p>{@code table-list} 在构造时归一化为小写 {@code db.table} 集合，供 {@link
 * org.apache.seatunnel.connectors.seatunnel.dts.source.convert.DtsRecordConverter} 过滤使用。
 */
public class DtsSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String brokerUrl;
    private final String topic;
    private final String sid;
    private final String user;
    private final String password;
    private final String checkpoint;
    private final boolean forceCheckpoint;
    private final int maxPollRecords;
    private final int queueCapacity;
    private final boolean dryRun;
    private final boolean skipColumnsJson;
    private final int maxColumnJsonLength;
    private final int pollBatchSize;
    private final Set<String> tableList;

    public DtsSourceConfig(ReadonlyConfig config) {
        this.brokerUrl = config.get(DtsSourceOptions.BROKER_URL);
        this.topic = config.get(DtsSourceOptions.TOPIC);
        this.sid = config.get(DtsSourceOptions.SID);
        this.user = config.get(DtsSourceOptions.USER);
        this.password = config.get(DtsSourceOptions.PASSWORD);
        this.checkpoint = config.get(DtsSourceOptions.CHECKPOINT);
        this.forceCheckpoint = config.get(DtsSourceOptions.FORCE_CHECKPOINT);
        this.maxPollRecords = config.get(DtsSourceOptions.MAX_POLL_RECORDS);
        this.queueCapacity = config.get(DtsSourceOptions.QUEUE_CAPACITY);
        this.dryRun = config.get(DtsSourceOptions.DRY_RUN);
        this.skipColumnsJson = config.get(DtsSourceOptions.SKIP_COLUMNS_JSON);
        this.maxColumnJsonLength = config.get(DtsSourceOptions.MAX_COLUMN_JSON_LENGTH);
        this.pollBatchSize = config.get(DtsSourceOptions.POLL_BATCH_SIZE);
        this.tableList = normalizeTableList(config.get(DtsSourceOptions.TABLE_LIST));
    }

    /** trim + 小写；空条目丢弃。空集合表示不过滤。 */
    private static Set<String> normalizeTableList(List<String> rawList) {
        if (rawList == null || rawList.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> normalized = new HashSet<>();
        for (String entry : rawList) {
            if (entry == null) {
                continue;
            }
            String trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            normalized.add(trimmed.toLowerCase(Locale.ROOT));
        }
        return normalized.isEmpty() ? Collections.emptySet() : normalized;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public String getTopic() {
        return topic;
    }

    public String getSid() {
        return sid;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getCheckpoint() {
        return checkpoint;
    }

    public boolean isForceCheckpoint() {
        return forceCheckpoint;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public boolean isSkipColumnsJson() {
        return skipColumnsJson;
    }

    public int getMaxColumnJsonLength() {
        return maxColumnJsonLength;
    }

    public int getPollBatchSize() {
        return pollBatchSize;
    }

    public Set<String> getTableList() {
        return tableList;
    }
}
