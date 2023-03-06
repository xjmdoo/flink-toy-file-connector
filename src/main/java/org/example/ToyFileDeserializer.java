package org.example;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class ToyFileDeserializer implements DeserializationSchema<RowData> {

    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    public ToyFileDeserializer(
            List<LogicalType> parsingTypes,
            DynamicTableSource.DataStructureConverter converter,
            TypeInformation<RowData> producedTypeInfo) {
        this.parsingTypes = parsingTypes;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        final String[] columns = new String(bytes).split(Pattern.quote(","));
        final RowKind kind = RowKind.INSERT;
        final Row row = new Row(kind, parsingTypes.size());
        for (int i = 0; i < parsingTypes.size(); i++) {
            row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i]));
        }
        return (RowData) converter.toInternal(row);
    }

    private static Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.valueOf(value);
            case VARCHAR:
                return value;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
