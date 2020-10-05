package Engine.Operators.Count;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.Collections;

public class CountLocalTupleProcessor implements TupleProcessor {

    private boolean nextFlag = false;
    private int counter = 0;

    @Override
    public void accept(Tuple tuple){
        counter++;
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {
        nextFlag = true;
    }

    @Override
    public void initialize() {

    }

    @Override
    public boolean hasNext() {
        return nextFlag;
    }

    @Override
    public Tuple next() {
        nextFlag = false;
        return Tuple.fromJavaList(Collections.singletonList(counter));
    }

    @Override
    public void dispose() {

    }
}
