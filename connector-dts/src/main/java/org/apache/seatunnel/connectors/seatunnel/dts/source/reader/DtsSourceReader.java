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

public class DtsSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(DtsSourceReader.class);
    private static final long POLL_TIMEOUT_MS = 100L;

    private final DtsSourceConfig config;
    private final DtsRecordQueue queue;
    private DtsConsumerRunner consumerRunner;
    private volatile long lastSourceTimestamp;
    private volatile long lastOffset;

    public DtsSourceReader(DtsSourceConfig config, DtsRecordQueue queue) {
        this.config = config;
        this.queue = queue;
    }

    @Override
    public void open() throws Exception {
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
        SeaTunnelRow row = queue.poll(POLL_TIMEOUT_MS);
        if (row == null) {
            return;
        }
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
