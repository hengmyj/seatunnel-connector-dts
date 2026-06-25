package org.apache.seatunnel.connectors.seatunnel.dts.source.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;

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
}
