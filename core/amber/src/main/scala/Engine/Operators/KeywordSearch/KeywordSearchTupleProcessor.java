package Engine.Operators.KeywordSearch;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.tuple.Tuple;
import Engine.Common.TupleProcessor;

public class KeywordSearchTupleProcessor implements TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private int targetField;
    private String keyword;

    KeywordSearchTupleProcessor(int targetField, String keyword){
        this.targetField = targetField;
        this.keyword = keyword;
    }

    public void setPredicate(int targetField, String keyword) {
        this.targetField = targetField;
        this.keyword = keyword;
    }

    @Override
    public void accept(Tuple tuple) {
        if(tuple.getString(targetField).toLowerCase().contains(keyword)){
            nextFlag = true;
            this.tuple = tuple;
        }
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
    public void initialize() {

    }

    @Override
    public boolean hasNext() {
        return nextFlag;
    }

    @Override
    public Tuple next() {
        nextFlag = false;
        return tuple;
    }

    @Override
    public void dispose() {

    }
}
