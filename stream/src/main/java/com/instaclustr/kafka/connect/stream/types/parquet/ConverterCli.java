package com.instaclustr.kafka.connect.stream.types.parquet;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.StreamParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.File;
import java.io.IOException;

public class ConverterCli {
    public static void main(String[] args) throws IOException {
        File file = new File(args[0]);
        SeekableInputStream fileStream = new LocalInputFile(file.toPath()).newStream();

        StreamParquetReader reader = new StreamParquetReader(new StreamInputFile(() -> fileStream, file.length()));

        int i = 0;
        while (true) {
            System.out.println("Progress: " + reader.getProgress());
            SimpleGroup record = (SimpleGroup) reader.read();
            System.out.println("\nParquet record " + i + ": " + record);
            if (record == null) {
                break;
            }

            ParquetKafkaDataConverter converter = ParquetKafkaDataConverter.newConverter();
            Struct struct = converter.convert(record);

            SourceRecord sourceRecord = new SourceRecord(
                    null, 
                    null, 
                    "mytopic",
                    null,
                    null,
                    null,
                    struct.schema(),
                    struct,
                    System.currentTimeMillis()
                    );

            System.out.println("Kafka record " + i + ": " + sourceRecord);
            i++;
        }

        System.out.println("Close reader");
        reader.close();
    }
}
