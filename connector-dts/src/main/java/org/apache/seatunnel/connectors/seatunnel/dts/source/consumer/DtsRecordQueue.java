package org.apache.seatunnel.connectors.seatunnel.dts.source.consumer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DtsRecordQueue {

    private final BlockingQueue<SeaTunnelRow> queue;

    public DtsRecordQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public void put(SeaTunnelRow row) throws InterruptedException {
        queue.put(row);
    }

    public SeaTunnelRow poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public int size() {
        return queue.size();
    }
}
