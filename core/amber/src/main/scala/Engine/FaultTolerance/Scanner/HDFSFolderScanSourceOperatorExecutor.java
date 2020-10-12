package Engine.FaultTolerance.Scanner;

import Engine.Common.tuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.SourceOperatorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import scala.*;
import scala.collection.*;
import scala.collection.Iterable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.collection.immutable.Stream;
import scala.collection.immutable.Vector;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.StringBuilder;
import scala.math.Numeric;
import scala.math.Ordering;
import scala.reflect.ClassTag;

import javax.print.URIException;
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
