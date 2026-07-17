package org.apache.seatunnel.connectors.seatunnel.dts.source;

import com.google.auto.service.AutoService;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.dts.source.config.DtsSourceOptions;

import java.io.Serializable;

/**
 * SPI 工厂：通过 {@link AutoService} 注册，使 SeaTunnel 能按插件名 {@code Dts} 发现本 Source。
 */
@AutoService(Factory.class)
public class DtsSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return DtsSourceOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DtsSourceOptions.BROKER_URL,
                        DtsSourceOptions.TOPIC,
                        DtsSourceOptions.SID,
                        DtsSourceOptions.USER,
                        DtsSourceOptions.PASSWORD)
                .optional(
                        DtsSourceOptions.CHECKPOINT,
                        DtsSourceOptions.FORCE_CHECKPOINT,
                        DtsSourceOptions.MAX_POLL_RECORDS,
                        DtsSourceOptions.QUEUE_CAPACITY,
                        DtsSourceOptions.DRY_RUN,
                        DtsSourceOptions.SKIP_COLUMNS_JSON,
                        DtsSourceOptions.MAX_COLUMN_JSON_LENGTH,
                        DtsSourceOptions.POLL_BATCH_SIZE,
                        DtsSourceOptions.TABLE_LIST)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new DtsSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DtsSource.class;
    }
}
