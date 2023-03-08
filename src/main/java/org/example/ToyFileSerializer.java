package org.example;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.List;

public class ToyFileSerializer implements SerializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;

    public ToyFileSerializer(List<LogicalType> parsingTypes) {
        this.parsingTypes = parsingTypes;
    }

    @Override
    public byte[] serialize(RowData element) {
        List<String> cols = new ArrayList<>();
        for (int i = 0; i < parsingTypes.size(); i++) {
            cols.add(parse(i, parsingTypes.get(i).getTypeRoot(), element));
        }
        String rowString = String.join(",", cols) + System.lineSeparator();
        return rowString.getBytes();
    }

    private String parse(int pos, LogicalTypeRoot root, RowData element) {
        switch (root) {
            case INTEGER:
                return String.valueOf(element.getInt(pos));
            case VARCHAR:
                return element.getString(pos).toString();
            default:
                throw new IllegalArgumentException();
        }
    }
}
