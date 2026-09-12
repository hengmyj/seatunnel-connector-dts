package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.common.UserCommitCallBack;
import com.aliyun.dts.subscribe.clients.formats.avro.Field;
import com.aliyun.dts.subscribe.clients.formats.avro.Operation;
import com.aliyun.dts.subscribe.clients.formats.avro.Record;
import com.aliyun.dts.subscribe.clients.formats.avro.Source;
import com.aliyun.dts.subscribe.clients.formats.avro.SourceType;
import com.aliyun.dts.subscribe.clients.record.fast.LazyParseRecordImpl;
import com.aliyun.dts.subscribe.clients.record.fast.LazyRecordDeserializer;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 本地微基准（DTS 订阅 Avro 记录，不是 MySQL binlog / Debezium / dump）。
 *
 * <p>同一条 DTS Avro {@code byte[]} 反复包成 {@link LazyParseRecordImpl} 再 convert。无 live DTS 集群。
 * skip = table-list 未命中（只解 header 库表）；hit = 命中并构建 {@code _columns_json}（解码 before/after images）。
 */
public class DtsLazyParseMicrobenchTest {

    private static final UserCommitCallBack NOOP_COMMIT =
            (tp, ts, offset, metadata) -> {
            };
    private static final TopicPartition TP = new TopicPartition("dts-topic", 0);

    @Test
    public void microbenchHeaderSkipVsFullImageParse() throws Exception {
        Set<String> whitelist = new HashSet<String>();
        whitelist.add("mydb.orders");
        DtsRecordConverter skipConverter = new DtsRecordConverter(whitelist);
        DtsRecordConverter hitConverter = new DtsRecordConverter(whitelist, false);

        benchAndPrint("payload=256B", 256, 50_000, 300_000, skipConverter, hitConverter);
        benchAndPrint("payload=8KiB", 8 * 1024, 20_000, 120_000, skipConverter, hitConverter);
    }

    private static void benchAndPrint(
            String label,
            int payloadBytes,
            int warmup,
            int iters,
            DtsRecordConverter skipConverter,
            DtsRecordConverter hitConverter)
            throws Exception {
        byte[] skipBytes = encodeAvro("otherdb.noise", payloadBytes);
        byte[] hitBytes = encodeAvro("mydb.orders", payloadBytes);

        LazyParseRecordImpl probe = wrap(skipBytes, 0L);
        Assert.assertTrue(skipConverter.convert(probe).isSkipped());
        Assert.assertFalse(isPayloadInited(probe));
        probe = wrap(hitBytes, 0L);
        Assert.assertFalse(hitConverter.convert(probe).isSkipped());
        Assert.assertTrue(isPayloadInited(probe));

        runLoop(warmup, skipBytes, skipConverter);
        runLoop(warmup, hitBytes, hitConverter);

        long skipNs = runLoop(iters, skipBytes, skipConverter);
        long hitNs = runLoop(iters, hitBytes, hitConverter);
        double skipRps = recordsPerSec(iters, skipNs);
        double hitRps = recordsPerSec(iters, hitNs);

        String line =
                String.format(
                        java.util.Locale.ROOT,
                        "MICROBENCH %s avroBytes=%d skip=%.0f rec/s hit=%.0f rec/s iters=%d skipNs=%d hitNs=%d",
                        label,
                        skipBytes.length,
                        skipRps,
                        hitRps,
                        iters,
                        skipNs,
                        hitNs);
        System.out.println(line);
        System.err.println(line);
        Assert.assertTrue("skip path should finish", skipNs > 0);
        Assert.assertTrue("hit path should finish", hitNs > 0);
    }

    private static long runLoop(int iters, byte[] avro, DtsRecordConverter converter) {
        long start = System.nanoTime();
        for (int i = 0; i < iters; i++) {
            converter.convert(wrap(avro, i));
        }
        return System.nanoTime() - start;
    }

    private static double recordsPerSec(int iters, long nanos) {
        if (nanos <= 0) {
            return 0D;
        }
        return iters * 1_000_000_000.0D / nanos;
    }

    private static byte[] encodeAvro(String objectName, int payloadBytes) throws Exception {
        char[] buf = new char[payloadBytes];
        Arrays.fill(buf, 'x');
        Record avro =
                Record.newBuilder()
                        .setVersion(1)
                        .setId(7L)
                        .setSourceTimestamp(1710000000L)
                        .setSourcePosition("mysql-bin.000001:4")
                        .setSafeSourcePosition("")
                        .setSourceTxid("1")
                        .setSource(new Source(SourceType.MySQL, "8.0"))
                        .setOperation(Operation.INSERT)
                        .setObjectName(objectName)
                        .setProcessTimestamps(null)
                        .setTags(Collections.emptyMap())
                        .setFields(Arrays.asList(new Field("id", 253), new Field("name", 253)))
                        .setBeforeImages(null)
                        .setAfterImages(new String(buf))
                        .build();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Record> writer = new SpecificDatumWriter<Record>(Record.class);
        writer.write(avro, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static LazyParseRecordImpl wrap(byte[] avro, long offset) {
        return new LazyParseRecordImpl(
                TP, avro, offset, new LazyRecordDeserializer(false), NOOP_COMMIT);
    }

    private static boolean isPayloadInited(LazyParseRecordImpl record) throws Exception {
        java.lang.reflect.Field field = LazyParseRecordImpl.class.getDeclaredField("initPayload");
        field.setAccessible(true);
        return field.getBoolean(record);
    }
}
