package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.UserRecord;
import com.aliyun.dts.subscribe.clients.record.fast.LazyParseRecordImpl;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * 将 DTS SDK {@link UserRecord} 转为信封行 {@link SeaTunnelRow}。
 *
 * <p>catalog-first：先用 header（{@code objectName} / {@code getDatabaseName}/{@code
 * getTableName}）做 {@code table-list} 过滤；未命中则 {@link ConvertResult#skipAndCommit()}，不调用
 * {@code getAfterImage}/{@code getBeforeImage}/{@code getFields}，从而不触发公开 SDK {@code
 * LazyParseRecordImpl} 的 Avro payload（fields + before/after images）。仅白名单命中表才解析行镜像并打包信封。
 *
 * <p>仅输出 INSERT/UPDATE/DELETE；DDL 与其它操作类型走 {@link ConvertResult#skip()}（不 commit）。
 */
public class DtsRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DtsRecordConverter.class);

    /** 小写 {@code db.table} 表白名单；空集合表示不过滤、接收全部表。 */
    private final Set<String> tableWhitelist;
    /** 为 true 时 {@code _columns_json} 固定为 {@code {}}，且不解析行镜像。 */
    private final boolean skipColumnsJson;
    /** 单列 JSON 最大字符数，传给 {@link DtsValueMapper#rowImageToJson}；0 表示不截断。 */
    private final int maxColumnJsonLength;

    public DtsRecordConverter() {
        this(Collections.emptySet(), false, 262144);
    }

    public DtsRecordConverter(Set<String> tableWhitelist) {
        this(tableWhitelist, false, 262144);
    }

    public DtsRecordConverter(Set<String> tableWhitelist, boolean skipColumnsJson) {
        this(tableWhitelist, skipColumnsJson, 262144);
    }

    public DtsRecordConverter(
            Set<String> tableWhitelist, boolean skipColumnsJson, int maxColumnJsonLength) {
        this.tableWhitelist =
                tableWhitelist == null ? Collections.emptySet() : tableWhitelist;
        this.skipColumnsJson = skipColumnsJson;
        this.maxColumnJsonLength = maxColumnJsonLength;
    }

    /**
     * 将一条 SDK 记录转为 {@link SeaTunnelRow} 或跳过结果。
     *
     * <p>顺序：操作类型（header）→ 仅用 header 库表做白名单 → 未命中 commit 且不碰 images → 命中后再按需解析
     * payload 构建 {@code _columns_json}。
     */
    public ConvertResult convert(UserRecord record) {
        OperationType operationType = record.getOperationType();
        if (operationType == null) {
            return ConvertResult.skip();
        }
        String op = operationType.name();
        if ("DDL".equals(op)) {
            LOG.debug("Skip DDL record at offset {}", kafkaOffset(record));
            return ConvertResult.skip();
        }
        if (!"INSERT".equals(op) && !"UPDATE".equals(op) && !"DELETE".equals(op)) {
            return ConvertResult.skip();
        }

        RecordSchema schema = headerSchema(record);
        String database = headerName(schema == null ? null : schema.getDatabaseName());
        String table = headerName(schema == null ? null : schema.getTableName());
        if (!isTableAllowed(database, table)) {
            // 不在白名单：推进位点；禁止碰 fields / images（否则会 decode Avro payload）。
            return ConvertResult.skipAndCommit();
        }

        String columnsJson = "{}";
        if (!skipColumnsJson) {
            // INSERT/UPDATE 用 after；DELETE 通常无 after，回退 before（否则 _columns_json 为空）。
            RowImage image = record.getAfterImage();
            if (image == null) {
                image = record.getBeforeImage();
            }
            columnsJson = DtsValueMapper.rowImageToJson(schema, image, maxColumnJsonLength);
        }

        Object[] fields =
                new Object[] {
                    database,
                    table,
                    op,
                    record.getSourceTimestamp(),
                    kafkaOffset(record),
                    columnsJson
                };
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(database + "." + table);
        return ConvertResult.of(row);
    }

    /**
     * 只取 header 侧 schema。{@link LazyParseRecordImpl#getSchema()} 在 schema 为空时会 {@code
     * initPayload}；此处用 {@code getSchema(false)}，依赖调用方已通过 {@code getOperationType()} 完成 header。
     */
    static RecordSchema headerSchema(UserRecord record) {
        if (record instanceof LazyParseRecordImpl) {
            return ((LazyParseRecordImpl) record).getSchema(false);
        }
        return record.getSchema();
    }

    static String headerName(
            com.aliyun.dts.subscribe.clients.common.NullableOptional<String> optional) {
        if (optional == null || !optional.isPresent()) {
            return "";
        }
        String value = optional.get();
        return value == null ? "" : value;
    }

    /**
     * Kafka offset。{@link UserRecord} 无此方法；懒解析实现是 {@link LazyParseRecordImpl#offset()}（构造即有，不触发
     * payload），旧实现是 {@link DefaultUserRecord#getOffset()}。
     */
    static long kafkaOffset(UserRecord record) {
        if (record instanceof LazyParseRecordImpl) {
            return ((LazyParseRecordImpl) record).offset();
        }
        if (record instanceof DefaultUserRecord) {
            return ((DefaultUserRecord) record).getOffset();
        }
        return 0L;
    }

    /**
     * 白名单为空，或 {@code database.table}（小写）在白名单内时返回 true。
     *
     * <p>SQL Server 公开 SDK 会把名字包成 {@code [db]} / {@code [schema].[table]}；匹配时去掉方括号，并同时尝试
     * {@code db.schema.table} 与 {@code db.table}（末段表名）。
     */
    boolean isTableAllowed(String database, String table) {
        if (tableWhitelist.isEmpty()) {
            return true;
        }
        String db = stripSqlServerBrackets(database);
        String tb = stripSqlServerBrackets(table);
        if (tb.isEmpty()) {
            return false;
        }
        String fullId = (db + "." + tb).toLowerCase(Locale.ROOT);
        if (tableWhitelist.contains(fullId)) {
            return true;
        }
        int lastDot = tb.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < tb.length() - 1) {
            String shortId = (db + "." + tb.substring(lastDot + 1)).toLowerCase(Locale.ROOT);
            return tableWhitelist.contains(shortId);
        }
        return false;
    }

    static String stripSqlServerBrackets(String name) {
        if (name == null || name.isEmpty()) {
            return "";
        }
        if (name.indexOf('[') < 0 && name.indexOf(']') < 0) {
            return name;
        }
        StringBuilder sb = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c != '[' && c != ']') {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static final class ConvertResult {
        private final SeaTunnelRow row;
        private final boolean skipped;
        /** 为 true 时虽跳过输出，调用方仍应 {@code record.commit()} 推进位点。 */
        private final boolean commitSkipped;

        private ConvertResult(SeaTunnelRow row, boolean skipped, boolean commitSkipped) {
            this.row = row;
            this.skipped = skipped;
            this.commitSkipped = commitSkipped;
        }

        /** 正常 DML 行，待入队。 */
        public static ConvertResult of(SeaTunnelRow row) {
            return new ConvertResult(row, false, false);
        }

        /** DDL / 心跳 / 未知操作：跳过且不 commit（大量堆积时 SDK 可能卡住）。 */
        public static ConvertResult skip() {
            return new ConvertResult(null, true, false);
        }

        /** table-list 未命中：不输出但 commit 以推进位点。 */
        public static ConvertResult skipAndCommit() {
            return new ConvertResult(null, true, true);
        }

        public boolean isSkipped() {
            return skipped;
        }

        public boolean shouldCommitSkipped() {
            return commitSkipped;
        }

        public SeaTunnelRow getRow() {
            return row;
        }
    }
}
