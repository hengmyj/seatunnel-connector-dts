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

/**
 * 在后台线程运行 {@link DefaultDTSConsumer}；SDK 回调中转换记录并写入有界队列，供 {@link
 * org.apache.seatunnel.connectors.seatunnel.dts.source.reader.DtsSourceReader} 拉取。
 *
 * <p>订阅模式固定 {@link ConsumerContext.ConsumerSubscribeMode#ASSIGN}。{@code record.commit()}
 * 必须在 {@code queue.put} 成功之后调用，否则背压失效且可能丢未入队数据。DDL 等 {@code skip()}
 * 路径故意不 commit——大量 DDL 段可能把 SDK 内部队列堵到约 512。
 */
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
        this.converter =
                new DtsRecordConverter(
                        config.getTableList(),
                        config.isSkipColumnsJson(),
                        config.getMaxColumnJsonLength());
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
        // 强制位点：配合删 localCheckpointStore-{sid} 使用；控制台显示的是已提交位点，不代表本次 seek 目标。
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
                                // 跳过整个 convert（无 table-list、无 JSON），仍 commit 推进位点。
                                dryRunCount.incrementAndGet();
                                record.commit("");
                                return;
                            }

                            DtsRecordConverter.ConvertResult result = converter.convert(record);
                            if (result.isSkipped()) {
                                long filtered = filteredCount.incrementAndGet();
                                if (result.shouldCommitSkipped()) {
                                    record.commit("");
                                }
                                // skip() 路径（DDL 等）：此处不 commit。
                                if (filtered % 1000 == 0) {
                                    LOG.info("DTS filtered_tables={}", filtered);
                                }
                                return;
                            }

                            SeaTunnelRow row = result.getRow();
                            queue.put(row); // 队列满时阻塞，对 SDK 形成背压
                            record.commit(""); // 入队成功后再 commit
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
                "DTS consumer started: sid={}, checkpoint={}, forceCheckpoint={}, tableList={}",
                config.getSid(),
                config.getCheckpoint(),
                config.isForceCheckpoint(),
                config.getTableList().isEmpty() ? "ALL" : config.getTableList());
    }

    public long getCommittedCount() {
        return committedCount.get();
    }

    /**
     * 历史兼容：实际返回的是已 commit 条数，不是 DTS offset。Reader 快照位点取自行字段 {@code
     * _offset}。
     */
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
