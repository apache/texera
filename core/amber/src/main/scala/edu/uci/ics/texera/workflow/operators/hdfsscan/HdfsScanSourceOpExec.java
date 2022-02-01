package edu.uci.ics.texera.workflow.operators.hdfsscan;


import com.google.common.base.Verify;
import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.scanner.BufferedBlockReaderOld;
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
    private String hdfsRestApiPort;
    private String hdfsPath;
    private ArrayList<Object> indicesToKeep;
    private char separator;
    private BufferedBlockReaderOld reader = null;
    private Schema schema;
    private long startOffset;
    private long endOffset;
    private final boolean header;

    HdfsScanSourceOpExec(String host, String hdfsRestApiPort, String hdfsPath, long startOffset, long endOffset, char delimiter, Schema schema, ArrayList<Object> indicesToKeep, boolean header) {
        this.host = host;
        this.hdfsRestApiPort = hdfsRestApiPort;
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
                    if (!Constants.sortExperiment()) {
                        String[] res = reader.readLine();
                        if (res == null || Arrays.stream(res).anyMatch(f -> (f == null) || (f.trim().length() == 0))) {
                            return null;
                        }
                        Verify.verify(schema != null);
                        if (res.length != schema.getAttributes().size()) {
                            res = Stream.concat(Arrays.stream(res),
                                            IntStream.range(0, schema.getAttributes().size() - res.length).mapToObj(i -> null))
                                    .toArray(String[]::new);
                        }

                        return Tuple.newBuilder().add(schema, res).build();
                    } else {
                        String[] line = reader.readLine();
                        if (line == null || Arrays.stream(line).noneMatch(Objects::nonNull)) {
                            // discard tuple if it's null or it only contains null
                            // which means it will always discard Tuple(null) from readLine()
                            return null;
                        }
                        Object[] res = new Object[line.length];
                        for (int i = 0; i < line.length; i++) {
                            res[i] = line[i];
                        }
                        Verify.verify(schema != null);
                        for (int i = 0; i < res.length; i++) {
                            try {
                                Double temp = Double.parseDouble((String) res[i]);
                                res[i] = temp;
                            } catch (NumberFormatException e) {/** do nothing **/}
                        }
                        if (res.length != schema.getAttributes().size()) {
                            Object[] ret = new Object[schema.getAttributes().size()];
                            for (int j = 0; j < res.length; j++) {
                                ret[j] = res[j];
                            }
                            for (int j = res.length; j < ret.length; j++) {
                                ret[j] = "null";
                            }
                            res = ret;
                        }
                        return Tuple.newBuilder().add(schema, res).build();
                    }

                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (Exception e) {
                    throw e;
                }
            }

        };
    }

    @Override
    public void open() {
        try {
            System.out.println(startOffset + " " + endOffset);
            //FileSystem fs = FileSystem.get(new URI(host),new Configuration());
            //FSDataInputStream stream = fs.open(new Path(hdfsPath));
            //stream.seek(startOffset);
            URL url = new URL("http://" + host + ":" + hdfsRestApiPort + "/webhdfs/v1" + hdfsPath + "?op=OPEN&offset=" + startOffset);
            InputStream stream = url.openStream();
            reader = new BufferedBlockReaderOld(stream, endOffset - startOffset, separator, indicesToKeep);
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