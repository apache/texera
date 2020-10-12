package Engine.Operators.Scan.HDFSFileScan;

import Engine.Common.tuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;


public class HDFSFileScanTupleProducer implements TupleProducer {

    private String host;
    private String hdfsPath;
    private int[] indicesToKeep;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private long startOffset;
    private long endOffset;

    private HashMap<String,String> params = new HashMap<>();

    HDFSFileScanTupleProducer(String host, String hdfsPath, long startOffset, long endOffset, char delimiter, int[] indicesToKeep, TableMetadata metadata) {
        this.host = host;
        this.hdfsPath = hdfsPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.metadata = metadata;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public void initialize() throws Exception {
        System.out.println(startOffset + " " + endOffset);
        //FileSystem fs = FileSystem.get(new URI(host),new Configuration());
        //FSDataInputStream stream = fs.open(new Path(hdfsPath));
        //stream.seek(startOffset);
        URL url = new URL(host + "/webhdfs/v1" + hdfsPath + "?op=OPEN&offset=" + startOffset);
        InputStream stream = url.openStream();
        reader = new BufferedBlockReader(stream, endOffset - startOffset, separator, indicesToKeep);
        if (startOffset > 0)
            reader.readLine();

        updateParamMap();
    }

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
    }

    public void updateParamMap(){
        params.put("host", host);
        params.put("hdfsPath", hdfsPath);
        params.put("indicesToKeep", Arrays.toString(indicesToKeep));
        params.put("separator", Character.toString(separator));
        params.put("startOffset", Long.toString(startOffset));
        params.put("endOffset", Long.toString(endOffset));
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        String[] res = reader.readLine();
        if (res == null) {
            return null;
        }
        if (metadata != null) {
            return Tuple.fromJavaStringArray(res, metadata.tupleMetadata().fieldTypes());
        } else {
            return Tuple.fromJavaArray(res);
        }
    }

    @Override
    public void dispose() throws Exception {
        reader.close();
    }
}
