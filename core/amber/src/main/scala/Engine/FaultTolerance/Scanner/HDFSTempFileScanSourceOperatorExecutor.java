package Engine.FaultTolerance.Scanner;

import Engine.Common.tuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.SourceOperatorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.collection.Iterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSTempFileScanSourceOperatorExecutor implements SourceOperatorExecutor {

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;

    public HDFSTempFileScanSourceOperatorExecutor(String host, String hdfsPath, char delimiter, TableMetadata metadata) {
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
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
                    if (metadata != null) {
                        return Tuple.fromJavaStringArray(reader.readLine(), metadata.tupleMetadata().fieldTypes());
                    } else {
                        return Tuple.fromJavaArray(reader.readLine());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    @Override
    public void open() {
        try {
            FileSystem fs = FileSystem.get(new URI(host), new Configuration());
            long endOffset = fs.getFileStatus(new Path(hdfsPath)).getLen();
            InputStream stream = fs.open(new Path(hdfsPath));
            reader = new BufferedBlockReader(stream, endOffset, separator, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
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
