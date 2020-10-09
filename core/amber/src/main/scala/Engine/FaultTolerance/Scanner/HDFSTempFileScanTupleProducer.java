package Engine.FaultTolerance.Scanner;

import Engine.Common.tuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class HDFSTempFileScanTupleProducer implements TupleProducer{

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;

    public HDFSTempFileScanTupleProducer(String host, String hdfsPath, char delimiter, TableMetadata metadata){
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.metadata = metadata;
    }

    @Override
    public void initialize() throws Exception {
        FileSystem fs = FileSystem.get(new URI(host),new Configuration());
        long endOffset =fs.getFileStatus(new Path(hdfsPath)).getLen();
        InputStream stream = fs.open(new Path(hdfsPath));
        reader = new BufferedBlockReader(stream,endOffset,separator,null);
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext();
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
