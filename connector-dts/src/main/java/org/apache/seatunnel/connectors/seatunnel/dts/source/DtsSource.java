package org.apache.seatunnel.connectors.seatunnel.dts.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.dts.source.config.DtsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.dts.source.config.DtsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.dts.source.reader.DtsSourceReader;

import java.util.Collections;
import java.util.List;

/**
 * 阿里云 DTS 订阅 Source（插件名 {@code Dts}）。
 *
 * <p>输出固定信封 schema：{@code _database/_table/_op/_ts/_offset/_columns_json}。列数据在 {@code
 * _columns_json} 内，与 Jdbc Sink 的 {@code schema_save_mode} 直连不兼容，需 Transform 或后续
 * {@code debezium_json} 形态。
 *
 * <p>继承单 Split Source：同一 {@code sid} 同时只能有一个消费者，并行度应固定为 1。
 */
public class DtsSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final DtsSourceConfig config;
    private final CatalogTable catalogTable;

    public DtsSource(ReadonlyConfig pluginConfig) {
        this.config = new DtsSourceConfig(pluginConfig);
        // 信封字段顺序与 DtsRecordConverter / DtsSourceReader 快照下标约定一致，勿随意调整。
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {
                            "_database",
                            "_table",
                            "_op",
                            "_ts",
                            "_offset",
                            "_columns_json"
                        },
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.STRING_TYPE
                        });
        this.catalogTable = CatalogTableUtil.getCatalogTable(DtsSourceOptions.IDENTIFIER, rowType);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return DtsSourceOptions.IDENTIFIER;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        // 并行度由 HOCON env.parallelism=1 约束；本 Source 本身也是单 Split。
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext)
            throws Exception {
        return new DtsSourceReader(config);
    }
}
