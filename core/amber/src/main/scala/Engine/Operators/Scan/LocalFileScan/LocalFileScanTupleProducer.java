package Engine.Operators.Scan.LocalFileScan;

import Engine.Common.AmberTuple.AmberTuple;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TableMetadata;
import Engine.Common.TupleProducer;
import Engine.Operators.Scan.BufferedBlockReader;
import com.google.common.base.Splitter;
import org.tukaani.xz.SeekableFileInputStream;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;


public class LocalFileScanTupleProducer extends TupleProducer {

    private String localPath;
    private int[] indicesToKeep;
    private char separator;
    private TableMetadata metadata;
    private BufferedBlockReader reader = null;
    private long startOffset;
    private long endOffset;

    private HashMap<String,String> params = new HashMap<>();


    private String[] shrinkStringArray(String[] array, int[] indicesToKeep){
        String[] res = new String[indicesToKeep.length];
        for(int i=0;i<indicesToKeep.length;++i)
            res[i] = array[indicesToKeep[i]];
        return res;
    }

    LocalFileScanTupleProducer(String localPath, long startOffset,long endOffset, char delimiter, int[] indicesToKeep, TableMetadata metadata){
        this.localPath = localPath;
        this.separator = delimiter;
        this.indicesToKeep = indicesToKeep;
        this.metadata = metadata;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public void initialize() throws Exception {
        SeekableFileInputStream stream = new SeekableFileInputStream(localPath);
        stream.seek(startOffset);
        reader= new BufferedBlockReader(stream,endOffset-startOffset,separator,indicesToKeep);
        if(startOffset > 0)
            reader.readLine();

        updateParamMap();
    }

    public void updateParamMap() {
        params.put("localPath", localPath);
        params.put("indicesToKeep", Arrays.toString(indicesToKeep));
        params.put("separator", Character.toString(separator));
        params.put("startOffset", Long.toString(startOffset));
        params.put("endOffset", Long.toString(endOffset));
    }

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext();
    }

    @Override
    public Tuple next() throws IOException {
        String[] res = reader.readLine();
        if(res == null){
            return null;
        }
        if(metadata != null) {
            return Tuple.fromJavaStringArray(res,metadata.tupleMetadata().fieldTypes());
        }else{
            return Tuple.fromJavaArray(res);
        }
    }

    @Override
    public void dispose() throws IOException {
        reader.close();
    }
}
