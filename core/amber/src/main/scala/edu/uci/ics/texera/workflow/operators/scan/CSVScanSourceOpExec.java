package edu.uci.ics.texera.workflow.operators.scan;

import com.google.common.base.Verify;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.scanner.BufferedBlockReader;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import org.tukaani.xz.SeekableFileInputStream;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CSVScanSourceOpExec implements SourceOperatorExecutor {

    private final String localPath;
    private final char separator;
    private BufferedBlockReader reader = null;
    private final long startOffset;
    private final long endOffset;
    private final Schema schema;
    private final boolean hasHeader;

    CSVScanSourceOpExec(String localPath, long startOffset, long endOffset, char delimiter, Schema schema,
                        boolean hasHeader) {
        this.localPath = localPath;
        this.separator = delimiter;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.schema = schema;
        this.hasHeader = hasHeader;
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        return new Iterator<Tuple>() {

            @Override
            public boolean hasNext() {
                try {
                    return reader.hasNext();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Tuple next() {
                try {

                    // obtain String representation of each field
                    // a null value will present if omit in between fields, e.g., ['hello', null, 'world']
                    String[] fields = reader.readLine();
                    if (fields == null || Arrays.stream(fields).noneMatch(Objects::nonNull)) {
                        // discard tuple if it's null or it only contains null
                        // which means it will always discard Tuple(null) from readLine()
                        return null;
                    }

                    Verify.verify(schema != null);

                    // however the null values won't present if omitted in the end, we need to match nulls.
                    if (fields.length != schema.getAttributes().size()) {
                        fields = Stream.concat(Arrays.stream(fields),
                                IntStream.range(0, schema.getAttributes().size() - fields.length)
                                        .mapToObj(i -> null)).toArray(String[]::new);
                    }

                    // parse Strings into inferred AttributeTypes
                    Object[] parsedFields = new Object[fields.length];
                    for (int i = 0; i < fields.length; i++) {
                        String field = fields[i];
                        switch (schema.getAttributes().get(i).getType()) {
                            case INTEGER:
                                parsedFields[i] = Integer.valueOf(field);
                                break;
                            case DOUBLE:
                                parsedFields[i] = Double.valueOf(field);
                                break;
                            case BOOLEAN:
                                parsedFields[i] = Boolean.valueOf(field);
                                break;
                            case LONG:
                                parsedFields[i] = Long.valueOf(field);
                                break;
                            case STRING:
                            case TIMESTAMP:
                            case ANY:
                            default:
                                // keep it as a String
                                parsedFields[i] = field;
                        }
                    }
                    return Tuple.newBuilder().add(schema, parsedFields).build();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        };
    }


    @Override
    public void open() {
        try {
            SeekableFileInputStream stream = new SeekableFileInputStream(localPath);
            stream.seek(startOffset);
            reader = new BufferedBlockReader(stream, endOffset - startOffset, separator, null);
            // skip line if this worker reads from middle of a file
            if (startOffset > 0)
                reader.readLine();
            // skip line if this worker reads the start of a file, and the file has a header line
            if (startOffset == 0 && hasHeader) {
                reader.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

}
