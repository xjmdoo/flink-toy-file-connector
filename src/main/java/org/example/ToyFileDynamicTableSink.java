package org.example;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.File;

public class ToyFileDynamicTableSink implements DynamicTableSink {
    private final String path;
    private final DataType producesDataType;

    public ToyFileDynamicTableSink(String path, DataType producesDataType) {
        this.path = path;
        this.producesDataType = producesDataType;
    }

    private ToyFileDynamicTableSink(ToyFileDynamicTableSink toCopy) {
        this.path = toCopy.path;
        this.producesDataType = toCopy.producesDataType;
    }
    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final SerializationSchema<RowData> serializer = new ToyFileSerializer(producesDataType.getLogicalType().getChildren());
        final SinkFunction<RowData> sinkFunction = new ToyFileSinkFunction(new File(path, "db.txt").toString(), serializer);
        return SinkFunctionProvider.of(sinkFunction, 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new ToyFileDynamicTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "Toy File Table Sink";
    }
}
