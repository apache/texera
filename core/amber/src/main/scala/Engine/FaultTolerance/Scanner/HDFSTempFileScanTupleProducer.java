package Engine.FaultTolerance.Scanner;

import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

public class HDFSTempFileScanTupleProducer extends TupleProducer{

    private String host;
    private String hdfsPath;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;

    private HashMap<String,String> params = new HashMap<>();

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
        updateParamMap();
    }

    public void updateParamMap() throws Exception {
        params.put("host", host);
        params.put("hdfsPath", hdfsPath);
        params.put("separator", Character.toString(separator));
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext();
    }

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
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
