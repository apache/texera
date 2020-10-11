package Engine.Operators.KeywordSearch;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.HashMap;

public class KeywordSearchTupleProcessor extends TupleProcessor {

    private Tuple tuple = null;
    private boolean nextFlag = false;
    private int targetField;
    private String keyword;

    private HashMap<String,String> params = new HashMap<>();

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

    public void updateParamMap() {
        params.put("targetField", Integer.toString(targetField));
        params.put("keyword", keyword);
    }

    @Override
    public void initialize() {updateParamMap();}

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
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
