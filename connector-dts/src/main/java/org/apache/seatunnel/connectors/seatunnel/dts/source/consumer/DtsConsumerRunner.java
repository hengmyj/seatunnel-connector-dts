package org.apache.seatunnel.connectors.seatunnel.dts.source.consumer;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.DefaultDTSConsumer;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.dts.source.config.DtsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.dts.source.convert.DtsRecordConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DtsConsumerRunner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DtsConsumerRunner.class);

    private final DtsSourceConfig config;
    private final DtsRecordQueue queue;
    private final DtsRecordConverter converter;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong committedCount = new AtomicLong();
    private final AtomicLong filteredCount = new AtomicLong();
    private final AtomicLong dryRunCount = new AtomicLong();

    private DefaultDTSConsumer consumer;

    public DtsConsumerRunner(DtsSourceConfig config, DtsRecordQueue queue) {
        this.config = config;
        this.queue = queue;
        this.converter = new DtsRecordConverter();
    }

    public void start() {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("max.poll.records", String.valueOf(config.getMaxPollRecords()));

        ConsumerContext ctx =
                new ConsumerContext(
                        null,
                        config.getBrokerUrl(),
                        config.getTopic(),
                        config.getSid(),
                        config.getUser(),
                        config.getPassword(),
                        config.getCheckpoint(),
                        ConsumerContext.ConsumerSubscribeMode.ASSIGN,
                        kafkaProps);
        if (config.isForceCheckpoint()) {
            ctx.setForceUseCheckpoint(true);
        }

        consumer = new DefaultDTSConsumer(ctx);
        Map<String, RecordListener> listeners = new HashMap<>();
        listeners.put(
                "seatunnel",
                new RecordListener() {
                    @Override
                    public void consume(DefaultUserRecord record) {
                        if (!running.get()) {
                            return;
                        }
                        try {
                            if (config.isDryRun()) {
                                dryRunCount.incrementAndGet();
                                return;
                            }

                            DtsRecordConverter.ConvertResult result = converter.convert(record);
                            if (result.isSkipped()) {
                                filteredCount.incrementAndGet();
                                return;
                            }

                            SeaTunnelRow row = result.getRow();
                            queue.put(row);
                            record.commit("");
                            long count = committedCount.incrementAndGet();
                            if (count % 1000 == 0) {
                                LOG.info(
                                        "DTS committed {} records, queue size={}",
                                        count,
                                        queue.size());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            LOG.warn("DTS listener interrupted");
                        } catch (Exception e) {
                            LOG.error("DTS listener error: {}", e.getMessage(), e);
                        }
                    }
                });
        consumer.addRecordListeners(listeners);
        consumer.start();
        LOG.info(
                "DTS consumer started: sid={}, checkpoint={}, forceCheckpoint={}",
                config.getSid(),
                config.getCheckpoint(),
                config.isForceCheckpoint());
    }

    public long getCommittedCount() {
        return committedCount.get();
    }

    public long getLastOffset() {
        return committedCount.get();
    }

    @Override
    public void close() {
        running.set(false);
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.warn("Error closing DTS consumer: {}", e.getMessage());
            }
        }
        LOG.info(
                "DTS consumer closed. committed={}, filtered={}, dryRun={}",
                committedCount.get(),
                filteredCount.get(),
                dryRunCount.get());
    }
}
