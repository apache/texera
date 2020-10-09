package Engine.FaultTolerance.Scanner;

import Engine.Common.tuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.SourceOperatorExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HDFSFolderScanSourceOperatorExecutor implements SourceOperatorExecutor {

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private RemoteIterator<LocatedFileStatus> files = null;
    private FileSystem fs = null;

    public HDFSFolderScanSourceOperatorExecutor(String host, String hdfsPath, char delimiter, TableMetadata metadata){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
    }

    private void ReadNextFileIfExists() throws IOException {
        if(files.hasNext()) {
            Path current = files.next().getPath();
            long endOffset = fs.getFileStatus(current).getLen();
            InputStream stream = fs.open(current);
            reader = new BufferedBlockReader(stream,endOffset,separator,null);
        }
    }

    @Override
    public void initialize() throws Exception {
        fs = FileSystem.get(new URI(host),new Configuration());
        files = fs.listFiles(new Path(hdfsPath),true);
        ReadNextFileIfExists();
    }

    @Override
    public boolean hasNext() throws IOException {
        if(reader == null){
            ReadNextFileIfExists();
        }
        return reader != null && reader.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        if(metadata != null) {
            return Tuple.fromJavaStringArray(reader.readLine(), metadata.tupleMetadata().fieldTypes());
        }else{
            return Tuple.fromJavaArray(reader.readLine());
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
