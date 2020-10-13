package texera.operators.localscan;


import engine.faulttolerance.scanner.BufferedBlockReader;
import com.google.common.base.Verify;
import org.tukaani.xz.SeekableFileInputStream;
import scala.collection.Iterator;
import texera.common.operators.source.TexeraSourceOperatorExecutor;
import texera.common.tuple.TexeraTuple;
import texera.common.tuple.schema.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class LocalCsvScanSourceOpExec implements TexeraSourceOperatorExecutor {

    private final String localPath;
    private final char separator;
    private BufferedBlockReader reader = null;
    private final long startOffset;
    private final long endOffset;
    private Schema schema;
    private final boolean header;


    private String[] shrinkStringArray(String[] array, int[] indicesToKeep) {
        String[] res = new String[indicesToKeep.length];
        for (int i = 0; i < indicesToKeep.length; ++i)
            res[i] = array[indicesToKeep[i]];
        return res;
    }

    LocalCsvScanSourceOpExec(String localPath, long startOffset, long endOffset, char delimiter, Schema schema, boolean header) {
        this.localPath = localPath;
        this.separator = delimiter;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.schema = schema;
        this.header = header;
    }

    @Override
    public Iterator<TexeraTuple> produceTexeraTuple() {
        return new Iterator<TexeraTuple>() {

            @Override
            public boolean hasNext() {
                try {
                    return reader.hasNext();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public TexeraTuple next() {
                try {
                    String[] res = reader.readLine();
                    if (res == null) {
                        return null;
                    }
                    if (res.length == 1 && res[0].isEmpty()) {
                        return null;
                    }
                    Verify.verify(schema != null);
                    if (res.length != schema.getAttributes().size()) {
                        res = Stream.concat(Arrays.stream(res),
                                IntStream.range(0, schema.getAttributes().size() - res.length).mapToObj(i -> null))
                                .toArray(String[]::new);
                    }

                    return TexeraTuple.newBuilder().add(schema, res).build();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
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
            if (startOffset == 0 && header) {
                reader.readLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }
}
