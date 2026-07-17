package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;
import com.aliyun.dts.subscribe.clients.record.value.Value;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * 将 DTS SDK {@link DefaultUserRecord} 转为信封行 {@link SeaTunnelRow}。
 *
 * <p>仅输出 INSERT/UPDATE/DELETE；DDL 与其它操作类型走 {@link ConvertResult#skip()}（不
 * commit）。表白名单未命中走 {@link ConvertResult#skipAndCommit()}，避免位点卡住同时跳过昂贵的列
 * JSON 构建。
 */
public class DtsRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DtsRecordConverter.class);

    /** 小写 {@code db.table} 表白名单；空集合表示不过滤、接收全部表。 */
    private final Set<String> tableWhitelist;
    /** 为 true 时 {@code _columns_json} 固定为 {@code {}}，表过滤逻辑仍生效。 */
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
     * <p>顺序：操作类型过滤 → 通过 {@link RecordSchema} 元数据做 {@code table-list} 白名单（此时尚未
     * 构建列 JSON）→ 仅对白名单内 DML 可选构建 {@code _columns_json}。
     */
    public ConvertResult convert(DefaultUserRecord record) {
        OperationType operationType = record.getOperationType();
        if (operationType == null) {
            return ConvertResult.skip();
        }
        String op = operationType.name();
        if ("DDL".equals(op)) {
            LOG.debug("Skip DDL record at offset {}", record.getOffset());
            return ConvertResult.skip();
        }
        if (!"INSERT".equals(op) && !"UPDATE".equals(op) && !"DELETE".equals(op)) {
            return ConvertResult.skip();
        }

        RecordSchema schema = record.getSchema();
        String database =
                schema.getDatabaseName().isPresent() ? schema.getDatabaseName().get() : "";
        String table = schema.getTableName().isPresent() ? schema.getTableName().get() : "";
        if (!isTableAllowed(database, table)) {
            // 不在白名单：推进位点，不入队、不序列化列 JSON。
            return ConvertResult.skipAndCommit();
        }

        // INSERT/UPDATE 用 after；DELETE 通常无 after，回退 before（否则 _columns_json 为空）。
        RowImage image = record.getAfterImage();
        if (image == null) {
            image = record.getBeforeImage();
        }
        String columnsJson =
                skipColumnsJson
                        ? "{}"
                        : DtsValueMapper.rowImageToJson(schema, image, maxColumnJsonLength);

        Object[] fields =
                new Object[] {
                    database,
                    table,
                    op,
                    record.getSourceTimestamp(),
                    record.getOffset(),
                    columnsJson
                };
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(database + "." + table);
        return ConvertResult.of(row);
    }

    /** 白名单为空，或 {@code database.table}（小写）在白名单内时返回 true。 */
    boolean isTableAllowed(String database, String table) {
        if (tableWhitelist.isEmpty()) {
            return true;
        }
        String tableId = (database + "." + table).toLowerCase(Locale.ROOT);
        return tableWhitelist.contains(tableId);
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
