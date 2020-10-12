package Engine.Operators.GroupBy;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.tuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class GroupByGlobalTupleProcessor<T> implements TupleProcessor {

    private AggregationType aggregationType;

    private HashMap<T,Double> results;
    private HashMap<T,Integer> counts;

    private HashMap<String,String> params = new HashMap<>();
    private Iterator<Map.Entry<T,Double>> iterator = null;

    public GroupByGlobalTupleProcessor(AggregationType aggregationType){
        this.aggregationType = aggregationType;
    }


    @Override
    public void accept(Tuple tuple) throws Exception {
        T key = tuple.getAs(0);
        double value = tuple.getDouble(1);
        if(!results.containsKey(key)){
            results.put(key,value);
            counts.put(key,1);
        }else{
            switch (aggregationType) {
                case Min:
                    results.put(key,Math.min(results.get(key),value));
                    break;
                case Max:
                    results.put(key,Math.max(results.get(key),value));
                    break;
                case Average:
                    counts.put(key,counts.get(key)+1);
                case Count:
                case Sum:
                    results.put(key,results.get(key)+value);
                    break;
            }
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
        iterator = results.entrySet().iterator();
    }

    public void updateParamMap(){
        params.put("aggregationType",aggregationType.name());
        params.put("results",results.toString());
        params.put("counts",counts.toString());
    }

    @Override
    public void initialize() throws Exception {
        results = new HashMap<>();
        counts = new HashMap<>();
        updateParamMap();
    }

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
    }

    @Override
    public boolean hasNext() throws Exception {
        return iterator!= null && iterator.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        Map.Entry<T,Double> cur = iterator.next();
        switch (aggregationType) {
            case Min:
            case Max:
            case Sum:
            case Count:
                return Tuple.fromJavaArray(new Object[]{cur.getKey(),cur.getValue().toString()});
            case Average:
                return Tuple.fromJavaArray(new Object[]{cur.getKey(),Double.toString(cur.getValue()/counts.get(cur.getKey()))});
            default:
                return null;
        }
    }

    @Override
    public void dispose() throws Exception {

    }
}
