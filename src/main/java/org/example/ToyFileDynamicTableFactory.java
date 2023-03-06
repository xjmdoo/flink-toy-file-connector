package org.example;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class ToyFileDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final ConfigOption<String> PATH = ConfigOptions.key("path")
            .stringType()
            .noDefaultValue();
    @Override
    public String factoryIdentifier() {
        return "toy-file";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        final String path = options.get(PATH);

        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return new ToyFileDynamicTableSource(path, producedDataType);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        final ReadableConfig options = helper.getOptions();
        final String path = options.get(PATH);
        final DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new ToyFileDynamicTableSink(path, producedDataType);
    }

}
