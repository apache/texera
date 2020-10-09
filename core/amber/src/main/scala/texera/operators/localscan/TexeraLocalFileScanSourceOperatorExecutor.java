package texera.operators.localscan;

import Engine.Common.SourceOperatorExecutor;
import Engine.Common.tuple.Tuple;
import Engine.Common.tuple.texera.TexeraTuple;
import Engine.Common.tuple.texera.schema.Attribute;
import Engine.Common.tuple.texera.schema.AttributeType;
import Engine.Common.tuple.texera.schema.Schema;
import Engine.FaultTolerance.Scanner.BufferedBlockReader;
import org.tukaani.xz.SeekableFileInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class TexeraLocalFileScanSourceOperatorExecutor implements SourceOperatorExecutor {

    private final String localPath;
    private final int[] indicesToKeep;
    private final char separator;
    private BufferedBlockReader reader = null;
    private final long startOffset;
    private final long endOffset;
    private Schema outputSchema = null;


    private String[] shrinkStringArray(String[] array, int[] indicesToKeep) {
        String[] res = new String[indicesToKeep.length];
        for (int i = 0; i < indicesToKeep.length; ++i)
            res[i] = array[indicesToKeep[i]];
        return res;
    }

    TexeraLocalFileScanSourceOperatorExecutor(String localPath, long startOffset, long endOffset, char delimiter, int[] indicesToKeep) {
        this.localPath = localPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public void initialize() throws Exception {
        SeekableFileInputStream stream = new SeekableFileInputStream(localPath);
        stream.seek(startOffset);
        reader = new BufferedBlockReader(stream, endOffset - startOffset, separator, indicesToKeep);
        if (startOffset > 0)
            reader.readLine();
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws IOException {
        try {
            String[] res = reader.readLine();
            if (res == null) {
                return null;
            }
            if (res.length == 1 && res[0].isEmpty()) {
                return null;
            }
            if (outputSchema == null) {
                this.outputSchema = Schema.newBuilder().add(IntStream.range(0, res.length).
                        mapToObj(i -> new Attribute("c" + i, AttributeType.STRING))
                        .collect(Collectors.toList())).build();
            }
            if (res.length != outputSchema.getAttributes().size()) {
                res = Stream.concat(Arrays.stream(res),
                        IntStream.range(0, outputSchema.getAttributes().size() - res.length).mapToObj(i -> null))
                        .toArray(String[]::new);
            }

            return TexeraTuple.newBuilder().add(this.outputSchema, res).build();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Override
    public void dispose() throws IOException {
        reader.close();
    }
}
