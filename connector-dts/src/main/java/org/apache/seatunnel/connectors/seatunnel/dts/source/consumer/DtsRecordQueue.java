package org.apache.seatunnel.connectors.seatunnel.dts.source.consumer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * SDK 回调线程与 {@link
 * org.apache.seatunnel.connectors.seatunnel.dts.source.reader.DtsSourceReader} 之间的有界队列。
 *
 * <p>{@link #put} 在队列满时阻塞，从而对 DTS SDK 消费形成背压，避免无限堆积导致 OOM。
 */
public class DtsRecordQueue {

    private final BlockingQueue<SeaTunnelRow> queue;

    public DtsRecordQueue(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /** 队列满时阻塞，直至有空位或被中断。 */
    public void put(SeaTunnelRow row) throws InterruptedException {
        queue.put(row);
    }

    /** 超时未取到返回 null。 */
    public SeaTunnelRow poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public int size() {
        return queue.size();
    }
}
