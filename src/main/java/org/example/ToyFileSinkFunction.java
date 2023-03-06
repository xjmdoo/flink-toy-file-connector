package org.example;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;

public class ToyFileSinkFunction extends RichSinkFunction<RowData> {

    private final String path;
    private final SerializationSchema<RowData> serializer;
    private DataOutputStream out;

    public ToyFileSinkFunction(String path, SerializationSchema<RowData> serializer) {
        this.path = path;
        this.serializer = serializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        FileOutputStream fileOutputStream = new FileOutputStream(path, true);
        out = new DataOutputStream(new BufferedOutputStream(fileOutputStream));
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        out.write(serializer.serialize(value));
    }

    @Override
    public void close() throws Exception {
        out.close();
    }
}
