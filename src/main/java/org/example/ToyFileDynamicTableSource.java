package org.example;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ToyFileDynamicTableSource implements ScanTableSource {

    private final String path;
    private final DataType producedDataType;

    public ToyFileDynamicTableSource(String path, DataType producedDataType) {
        this.path = path;
        this.producedDataType = producedDataType;
    }


    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final TypeInformation<RowData> producedTypeInfo = scanContext.createTypeInformation(producedDataType);
        final DataStructureConverter converter = scanContext.createDataStructureConverter(producedDataType);
        final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();
        final DeserializationSchema<RowData> deserializer = new ToyFileDeserializer(parsingTypes, converter, producedTypeInfo);

        final SourceFunction<RowData> sourceFunction = new ToyFileSourceFunction(path, deserializer);

        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
