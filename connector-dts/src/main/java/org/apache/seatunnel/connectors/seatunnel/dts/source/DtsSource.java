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
import org.apache.seatunnel.connectors.seatunnel.dts.source.consumer.DtsRecordQueue;
import org.apache.seatunnel.connectors.seatunnel.dts.source.reader.DtsSourceReader;

import java.util.Collections;
import java.util.List;

public class DtsSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final DtsSourceConfig config;
    private final CatalogTable catalogTable;
    private final DtsRecordQueue queue;

    public DtsSource(ReadonlyConfig pluginConfig) {
        this.config = new DtsSourceConfig(pluginConfig);
        this.queue = new DtsRecordQueue(config.getQueueCapacity());
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
        // parallelism=1 enforced via env config; single-split source only
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext)
            throws Exception {
        return new DtsSourceReader(config, queue);
    }
}
