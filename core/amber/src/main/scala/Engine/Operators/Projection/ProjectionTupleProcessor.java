package Engine.Operators.Projection;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.tuple.Tuple;
import Engine.Common.TupleProcessor;


public class ProjectionTupleProcessor implements TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private int[] targetFields;

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

    @Override
    public void initialize() throws Exception {

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
