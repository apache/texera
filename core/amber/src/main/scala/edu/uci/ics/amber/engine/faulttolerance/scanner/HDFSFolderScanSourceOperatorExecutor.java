package edu.uci.ics.amber.engine.faulttolerance.scanner;

import edu.uci.ics.amber.engine.common.tuple.Tuple;
import edu.uci.ics.amber.engine.common.TableMetadata;
import edu.uci.ics.amber.engine.common.SourceOperatorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.collection.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSFolderScanSourceOperatorExecutor implements SourceOperatorExecutor {

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private RemoteIterator<LocatedFileStatus> files = null;
    private FileSystem fs = null;

    public HDFSFolderScanSourceOperatorExecutor(String host, String hdfsPath, char delimiter, TableMetadata metadata) {
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
    }

    private void ReadNextFileIfExists() throws IOException {
        if (files.hasNext()) {
            Path current = files.next().getPath();
            long endOffset = fs.getFileStatus(current).getLen();
            InputStream stream = fs.open(current);
            reader = new BufferedBlockReader(stream, endOffset, separator, null);
        }
    }


    @Override
    public void open() {
        try {
            fs = FileSystem.get(new URI(host), new Configuration());
            files = fs.listFiles(new Path(hdfsPath), true);
            ReadNextFileIfExists();
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

    @Override
    public Iterator<Tuple> produce() {
        return new Iterator<Tuple>() {
            @Override
            public boolean hasNext() {
                try {
                    if (reader == null) {
                        ReadNextFileIfExists();
                    }
                    return reader != null && reader.hasNext();
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
}
