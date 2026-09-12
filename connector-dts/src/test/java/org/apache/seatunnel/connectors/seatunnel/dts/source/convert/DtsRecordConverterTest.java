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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DtsRecordConverterTest {

    private static final UserCommitCallBack NOOP_COMMIT =
            (tp, ts, offset, metadata) -> {
            };

    @Test
    public void testTableWhitelistEmptyAllowsAll() {
        DtsRecordConverter converter = new DtsRecordConverter(Collections.emptySet());
        Assert.assertTrue(converter.isTableAllowed("db1", "t1"));
    }

    @Test
    public void testTableWhitelistMatch() {
        Set<String> whitelist = new HashSet<>();
        whitelist.add("db1.t1");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist);
        Assert.assertTrue(converter.isTableAllowed("db1", "t1"));
        Assert.assertTrue(converter.isTableAllowed("DB1", "T1"));
        Assert.assertFalse(converter.isTableAllowed("db1", "t2"));
        Assert.assertFalse(converter.isTableAllowed("db2", "t1"));
    }

    @Test
    public void testSqlServerBracketNamesMatchDbTable() {
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist);
        Assert.assertTrue(converter.isTableAllowed("[mydb]", "[dbo].[orders]"));
        Assert.assertTrue(converter.isTableAllowed("[mydb]", "[dbo].[ORDERS]"));
        Assert.assertFalse(converter.isTableAllowed("[mydb]", "[dbo].[users]"));
    }

    @Test
    public void whitelistMissDoesNotInitAvroPayload() throws Exception {
        String huge = repeat('x', 64 * 1024);
        LazyParseRecordImpl record =
                lazyRecord("otherdb.noise", SourceType.MySQL, huge, Collections.emptyMap());
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist);

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertTrue(result.isSkipped());
        Assert.assertTrue(result.shouldCommitSkipped());
        Assert.assertTrue(isHeaderInited(record));
        Assert.assertFalse(
                "filter miss must not decode Avro fields/images", isPayloadInited(record));
        Assert.assertEquals("otherdb", record.getSchema(false).getDatabaseName().orElse(""));
        Assert.assertEquals("noise", record.getSchema(false).getTableName().orElse(""));
        Assert.assertFalse(isPayloadInited(record));
    }

    @Test
    public void whitelistHitBuildsEnvelopeJsonAndInitsPayload() throws Exception {
        LazyParseRecordImpl record =
                lazyRecord("mydb.orders", SourceType.MySQL, "alice", Collections.emptyMap());
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist);

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertFalse(result.isSkipped());
        Assert.assertTrue(isPayloadInited(record));
        SeaTunnelRow row = result.getRow();
        Assert.assertEquals("mydb", row.getField(0));
        Assert.assertEquals("orders", row.getField(1));
        Assert.assertEquals("INSERT", row.getField(2));
        Assert.assertEquals(1710000000L, row.getField(3));
        Assert.assertEquals(42L, row.getField(4));
        String json = (String) row.getField(5);
        Assert.assertTrue(json.contains("alice"));
    }

    @Test
    public void skipColumnsJsonDoesNotInitAvroPayload() throws Exception {
        String huge = repeat('x', 64 * 1024);
        LazyParseRecordImpl record =
                lazyRecord("mydb.orders", SourceType.MySQL, huge, Collections.emptyMap());
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist, true);

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertFalse(result.isSkipped());
        Assert.assertFalse(
                "skip-columns-json must not call getAfterImage / decode images",
                isPayloadInited(record));
        Assert.assertEquals("{}", result.getRow().getField(5));
        Assert.assertEquals("mydb", result.getRow().getField(0));
        Assert.assertEquals("orders", result.getRow().getField(1));
    }

    @Test
    public void logicalTagNamesDriveWhitelist() throws Exception {
        Map<String, String> tags = new HashMap<>();
        tags.put("l_db_name", "mydb");
        tags.put("l_tb_name", "orders");
        LazyParseRecordImpl record =
                lazyRecord("physdb.phys_table", SourceType.MySQL, "alice", tags);
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist, true);

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertFalse(result.isSkipped());
        Assert.assertFalse(isPayloadInited(record));
        Assert.assertEquals("mydb", result.getRow().getField(0));
        Assert.assertEquals("orders", result.getRow().getField(1));
    }

    @Test
    public void sqlServerObjectNameMatchesDbTableWhitelistWithoutPayload() throws Exception {
        LazyParseRecordImpl record =
                lazyRecord("mydb.dbo.orders", SourceType.SQLServer, "alice", Collections.emptyMap());
        Set<String> whitelist = new HashSet<>();
        whitelist.add("mydb.orders");
        DtsRecordConverter converter = new DtsRecordConverter(whitelist, true);

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertFalse(result.isSkipped());
        Assert.assertFalse(isPayloadInited(record));
        Assert.assertEquals("[mydb]", result.getRow().getField(0));
        Assert.assertEquals("[dbo].[orders]", result.getRow().getField(1));
    }

    @Test
    public void heartbeatSkipsWithoutPayload() throws Exception {
        LazyParseRecordImpl record = lazyHeartbeat();
        DtsRecordConverter converter = new DtsRecordConverter(Collections.singleton("mydb.orders"));

        DtsRecordConverter.ConvertResult result = converter.convert(record);

        Assert.assertTrue(result.isSkipped());
        Assert.assertFalse(result.shouldCommitSkipped());
        Assert.assertFalse(isPayloadInited(record));
    }

    private static LazyParseRecordImpl lazyRecord(
            String objectName, SourceType sourceType, String nameValue, Map<String, String> tags)
            throws Exception {
        Record avro =
                Record.newBuilder()
                        .setVersion(1)
                        .setId(7L)
                        .setSourceTimestamp(1710000000L)
                        .setSourcePosition("mysql-bin.000001:4")
                        .setSafeSourcePosition("")
                        .setSourceTxid("1")
                        .setSource(new Source(sourceType, "8.0"))
                        .setOperation(Operation.INSERT)
                        .setObjectName(objectName)
                        .setProcessTimestamps(null)
                        .setTags(tags == null ? Collections.emptyMap() : tags)
                        .setFields(
                                Arrays.asList(
                                        new Field("id", 253), new Field("name", 253)))
                        .setBeforeImages(null)
                        .setAfterImages(nameValue)
                        .build();
        return wrap(avro, 42L);
    }

    private static LazyParseRecordImpl lazyHeartbeat() throws Exception {
        Record avro =
                Record.newBuilder()
                        .setVersion(1)
                        .setId(8L)
                        .setSourceTimestamp(1710000001L)
                        .setSourcePosition("mysql-bin.000001:99")
                        .setSafeSourcePosition("")
                        .setSourceTxid("")
                        .setSource(new Source(SourceType.MySQL, "8.0"))
                        .setOperation(Operation.HEARTBEAT)
                        .setObjectName(null)
                        .setProcessTimestamps(null)
                        .setTags(Collections.emptyMap())
                        .setFields(null)
                        .setBeforeImages(null)
                        .setAfterImages(null)
                        .build();
        return wrap(avro, 99L);
    }

    private static LazyParseRecordImpl wrap(Record avro, long offset) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Record> writer = new SpecificDatumWriter<Record>(Record.class);
        writer.write(avro, encoder);
        encoder.flush();
        return new LazyParseRecordImpl(
                new TopicPartition("dts-topic", 0),
                out.toByteArray(),
                offset,
                new LazyRecordDeserializer(false),
                NOOP_COMMIT);
    }

    private static boolean isPayloadInited(LazyParseRecordImpl record) throws Exception {
        java.lang.reflect.Field field = LazyParseRecordImpl.class.getDeclaredField("initPayload");
        field.setAccessible(true);
        return field.getBoolean(record);
    }

    private static boolean isHeaderInited(LazyParseRecordImpl record) throws Exception {
        java.lang.reflect.Field field = LazyParseRecordImpl.class.getDeclaredField("initHeader");
        field.setAccessible(true);
        return field.getBoolean(record);
    }

    private static String repeat(char c, int n) {
        char[] buf = new char[n];
        Arrays.fill(buf, c);
        return new String(buf);
    }
}
