package texera.operators.localscan;

import Engine.Common.SourceOperatorExecutor;
import Engine.Common.tuple.Tuple;
import Engine.Common.tuple.texera.TexeraTuple;
import Engine.Common.tuple.texera.schema.Attribute;
import Engine.Common.tuple.texera.schema.AttributeType;
import Engine.Common.tuple.texera.schema.Schema;
import Engine.FaultTolerance.Scanner.BufferedBlockReader;
import org.tukaani.xz.SeekableFileInputStream;
import scala.*;
import scala.collection.*;
import scala.collection.Iterable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.collection.immutable.Vector;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.StringBuilder;
import scala.math.Numeric;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class TexeraLocalFileScanSourceOpExec implements SourceOperatorExecutor {

    private final String localPath;
    private final int[] indicesToKeep;
    private final char separator;
    private BufferedBlockReader reader = null;
    private final long startOffset;
    private final long endOffset;
    private boolean header;
    private Schema outputSchema = null;


    private String[] shrinkStringArray(String[] array, int[] indicesToKeep) {
        String[] res = new String[indicesToKeep.length];
        for (int i = 0; i < indicesToKeep.length; ++i)
            res[i] = array[indicesToKeep[i]];
        return res;
    }

    TexeraLocalFileScanSourceOpExec(String localPath, long startOffset, long endOffset, char delimiter, int[] indicesToKeep, boolean header) {
        this.localPath = localPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.header = header;
    }

    @Override
    public Iterator<Tuple> produce() {
        return new Iterator<Tuple>() {

            @Override
            public boolean hasNext() {
                try {
                    return reader.hasNext();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public Tuple next() {
                try {
                    String[] res = reader.readLine();
                    if (res == null) {
                        return null;
                    }
                    if (res.length == 1 && res[0].isEmpty()) {
                        return null;
                    }
                    if (outputSchema == null) {
                        outputSchema = Schema.newBuilder().add(IntStream.range(0, res.length).
                                mapToObj(i -> new Attribute("c" + i, AttributeType.STRING))
                                .collect(Collectors.toList())).build();
                    }
                    if (res.length != outputSchema.getAttributes().size()) {
                        res = Stream.concat(Arrays.stream(res),
                                IntStream.range(0, outputSchema.getAttributes().size() - res.length).mapToObj(i -> null))
                                .toArray(String[]::new);
                    }

                    return TexeraTuple.newBuilder().add(outputSchema, res).build();
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
            reader = new BufferedBlockReader(stream, endOffset - startOffset, separator, indicesToKeep);
            if (startOffset > 0)
                reader.readLine();
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
