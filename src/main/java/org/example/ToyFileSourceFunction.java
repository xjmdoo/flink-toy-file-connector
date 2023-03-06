package org.example;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class ToyFileSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(ToyFileSourceFunction.class);

    private final File dbFile;
    private final DeserializationSchema<RowData> deserializer;
    public ToyFileSourceFunction(String path, DeserializationSchema<RowData> deserializer) {
        this.deserializer = deserializer;
        try {
            dbFile = new File(path, "db.txt");
            dbFile.getParentFile().mkdirs();
            boolean didCreateFile = dbFile.createNewFile();
            if (!didCreateFile) {
                LOG.info("DB file " + dbFile + " already exists");
            } else {
                LOG.info("Created DB file: " + dbFile);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        List<String> rows = Files.readAllLines(dbFile.toPath());
        LOG.info("Read " + rows.size() + "row(s) from DB file");
        for (String row : rows) {
            sourceContext.collect(deserializer.deserialize(row.getBytes()));
        }
    }

    @Override
    public void cancel() {}

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }
}
