package Engine.Operators.Projection;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.tuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.Arrays;
import java.util.HashMap;


public class ProjectionTupleProcessor implements TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private int[] targetFields;

    private HashMap<String,String> params = new HashMap<>();

    ProjectionTupleProcessor(int[] targetFields){
        this.targetFields = targetFields;
    }

    private Object[] subSequenceFromArray(Object[] source, int[] indices){
        Object[] result = new Object[indices.length];
        int cur = 0;
        for (int index : indices) {
            result[cur++] = source[index];
        }
        return result;
    }



    @Override
    public void accept(Tuple tuple) throws Exception {
        nextFlag = true;
        this.tuple = Tuple.fromJavaArray(subSequenceFromArray(tuple.toArray(),targetFields));
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {

    }

    public void updateParamMap() {
        params.put("targetFields", Arrays.toString(targetFields));
    }

    @Override
    public void initialize() throws Exception {updateParamMap();}

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
    }

    @Override
    public boolean hasNext() throws Exception {
        return nextFlag;
    }

    @Override
    public Tuple next() throws Exception {
        nextFlag = false;
        return tuple;
    }

    @Override
    public void dispose() throws Exception {

    }
}
