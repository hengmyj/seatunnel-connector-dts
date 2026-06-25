package org.apache.seatunnel.connectors.seatunnel.dts.source.convert;

import com.aliyun.dts.subscribe.clients.record.DefaultUserRecord;
import com.aliyun.dts.subscribe.clients.record.OperationType;
import com.aliyun.dts.subscribe.clients.record.RecordSchema;
import com.aliyun.dts.subscribe.clients.record.RowImage;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DtsRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(DtsRecordConverter.class);

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
        RowImage image = record.getAfterImage();
        if (image == null) {
            image = record.getBeforeImage();
        }
        String columnsJson = DtsValueMapper.rowImageToJson(schema, image);

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

    public static final class ConvertResult {
        private final SeaTunnelRow row;
        private final boolean skipped;

        private ConvertResult(SeaTunnelRow row, boolean skipped) {
            this.row = row;
            this.skipped = skipped;
        }

        public static ConvertResult of(SeaTunnelRow row) {
            return new ConvertResult(row, false);
        }

        public static ConvertResult skip() {
            return new ConvertResult(null, true);
        }

        public boolean isSkipped() {
            return skipped;
        }

        public SeaTunnelRow getRow() {
            return row;
        }
    }
}
