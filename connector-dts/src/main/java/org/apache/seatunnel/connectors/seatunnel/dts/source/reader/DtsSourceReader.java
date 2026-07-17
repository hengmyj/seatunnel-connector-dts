package org.apache.seatunnel.connectors.seatunnel.dts.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.dts.source.config.DtsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.dts.source.consumer.DtsConsumerRunner;
import org.apache.seatunnel.connectors.seatunnel.dts.source.consumer.DtsRecordQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 单 Split Reader：启动 {@link DtsConsumerRunner}，从有界队列批量拉取行交给引擎。
 *
 * <p>引擎 checkpoint 状态为 {@code offset@sourceTimestamp}（UTF-8）；与 HOCON {@code checkpoint}
 * 格式类似，但这里记录的是已 collect 行上的位点，不等于 SDK 内部已 commit 位点。
 */
public class DtsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(DtsSourceReader.class);
    /** 队列空时首次 poll 的等待时间；同批次后续用 0 避免空转。 */
    private static final long POLL_TIMEOUT_MS = 100L;

    private final DtsSourceConfig config;
    private DtsRecordQueue queue;
    private DtsConsumerRunner consumerRunner;
    private volatile long lastSourceTimestamp;
    private volatile long lastOffset;

    public DtsSourceReader(DtsSourceConfig config) {
        this.config = config;
    }

    @Override
    public void open() throws Exception {
        queue = new DtsRecordQueue(config.getQueueCapacity());
        consumerRunner = new DtsConsumerRunner(config, queue);
        consumerRunner.start();
        LOG.info("DtsSourceReader opened");
    }

    @Override
    public void close() throws IOException {
        if (consumerRunner != null) {
            consumerRunner.close();
        }
        LOG.info("DtsSourceReader closed");
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        int drained = 0;
        int batchSize = config.getPollBatchSize();
        // 一次最多 drain poll-batch-size 行，避免单次 pollNext 阻塞过久同时缓解队列积压。
        while (drained < batchSize) {
            SeaTunnelRow row = queue.poll(drained == 0 ? POLL_TIMEOUT_MS : 0);
            if (row == null) {
                break;
            }
            // collect 须在 checkpoint lock 内，保证与 snapshotState 一致。
            synchronized (output.getCheckpointLock()) {
                output.collect(row);
                if (row.getArity() >= 5) {
                    Object ts = row.getField(3);
                    Object offset = row.getField(4);
                    if (ts instanceof Number) {
                        lastSourceTimestamp = ((Number) ts).longValue();
                    }
                    if (offset instanceof Number) {
                        lastOffset = ((Number) offset).longValue();
                    }
                }
            }
            drained++;
        }
    }

    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) throws Exception {
        String state = lastOffset + "@" + lastSourceTimestamp;
        return state.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected void restoreState(byte[] restoredState) {
        if (restoredState == null || restoredState.length == 0) {
            return;
        }
        String state = new String(restoredState, StandardCharsets.UTF_8);
        int at = state.indexOf('@');
        if (at > 0) {
            try {
                lastOffset = Long.parseLong(state.substring(0, at));
                lastSourceTimestamp = Long.parseLong(state.substring(at + 1));
            } catch (NumberFormatException e) {
                LOG.warn("Unable to restore DTS reader state: {}", state);
            }
        }
    }
}
