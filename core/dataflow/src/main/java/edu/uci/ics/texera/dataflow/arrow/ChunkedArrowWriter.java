package edu.uci.ics.texera.dataflow.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


public class ChunkedArrowWriter<T> {

    private final int chunkSize;
    private final Vectorizer<T> vectorizer;

    public ChunkedArrowWriter(int chunkSize, Vectorizer<T> vectorizer) {
        this.chunkSize = chunkSize;
        this.vectorizer = vectorizer;
    }

    public void write(File file, List<T> values, Schema arrowSchema) throws IOException {
        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();

        try (RootAllocator allocator = new RootAllocator();
             VectorSchemaRoot schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator);
             FileOutputStream fd = new FileOutputStream(file);
             ArrowFileWriter fileWriter = new ArrowFileWriter(schemaRoot, dictProvider, fd.getChannel())) {

            fileWriter.start();

            int index = 0;
            while (index < values.size()) {
                schemaRoot.allocateNew();
                int chunkIndex = 0;
                while (chunkIndex < chunkSize && index + chunkIndex < values.size()) {
                    vectorizer.vectorize(values.get(index + chunkIndex), chunkIndex, schemaRoot);
                    chunkIndex++;
                }
                schemaRoot.setRowCount(chunkIndex);
                fileWriter.writeBatch();

                index += chunkIndex;
                schemaRoot.clear();
            }

            fileWriter.end();
        }
    }

    @FunctionalInterface
    public interface Vectorizer<T> {
        void vectorize(T value, int index, VectorSchemaRoot batch);
    }
}


