package edu.uci.ics.texera.workflow.operators.hdfsscan;


import com.google.common.base.Verify;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.common.TableMetadata;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.scanner.BufferedBlockReader;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class HdfsScanSourceOpExec implements SourceOperatorExecutor {

    private String host;
    private String hdfsPath;
    private ArrayList<Object> indicesToKeep;
    private char separator;
    private BufferedBlockReader reader = null;
    private Schema schema;
    private long startOffset;
    private long endOffset;
    private final boolean header;

    HdfsScanSourceOpExec(String host, String hdfsPath, long startOffset, long endOffset, char delimiter, Schema schema, ArrayList<Object> indicesToKeep, boolean header){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.schema = schema;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.header = header;
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
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
                    if(res == null || Arrays.stream(res).noneMatch(Objects::nonNull)){
                        return null;
                    }
                    Verify.verify(schema != null);
                    if (res.length != schema.getAttributes().size()) {
                        res = Stream.concat(Arrays.stream(res),
                                IntStream.range(0, schema.getAttributes().size() - res.length).mapToObj(i -> null))
                                .toArray(String[]::new);
                    }

                    return Tuple.newBuilder().add(schema, res).build();

                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch(Exception e){
                    throw e;
                }
            }

        };
    }

    @Override
    public void open() {
        try {
            System.out.println(startOffset+" "+endOffset);
            //FileSystem fs = FileSystem.get(new URI(host),new Configuration());
            //FSDataInputStream stream = fs.open(new Path(hdfsPath));
            //stream.seek(startOffset);
            URL url = new URL("http://"+ host+":9870/webhdfs/v1"+hdfsPath+"?op=OPEN&offset="+startOffset);
            InputStream stream = url.openStream();
            reader = new BufferedBlockReader(stream,endOffset-startOffset,separator,indicesToKeep);
            if (startOffset > 0) {
                reader.readLine();
            }
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
