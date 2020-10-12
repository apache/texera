package Engine.Operators.HashJoin;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.tuple.amber.AmberTuple;
import Engine.Common.tuple.Tuple;
import Engine.Common.TupleProcessor;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class HashJoinTupleProcessor<K> implements TupleProcessor {

    private LayerTag innerTableIdentifier;
    private int innerTableIndex;
    private int outerTableIndex;
    private boolean isCurrentTableInner = false;
    private boolean isInnerTableFinished = false;
    private HashMap<K, ArrayList<Object[]>> innerTableHashMap = null;
    private Iterator<Object[]> currentEntry = null;
    private Object[] currentTuple = null;

    private HashMap<String,String> params = new HashMap<>();

    HashJoinTupleProcessor(LayerTag innerTableIdentifier, int innerTableIndex, int outerTableIndex){
        this.innerTableIdentifier = innerTableIdentifier;
        this.innerTableIndex = innerTableIndex;
        this.outerTableIndex = outerTableIndex;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void accept(Tuple tuple) {
        if(isCurrentTableInner){
            K key = (K)tuple.get(innerTableIndex);
            if(!innerTableHashMap.containsKey(key)) {
                innerTableHashMap.put(key,new ArrayList<>());
            }
            innerTableHashMap.get(key).add(tuple.toArray());
        }else{
            if(!isInnerTableFinished) {
                throw new AssertionError();
            }else{
                K key = (K)tuple.get(outerTableIndex);
                if(innerTableHashMap.containsKey(key)) {
                    currentEntry = innerTableHashMap.get(key).iterator();
                    currentTuple = tuple.toArray();
                }
            }
        }
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {
        isCurrentTableInner = innerTableIdentifier.equals(from);
    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {
        isInnerTableFinished = innerTableIdentifier.equals(from);
    }

    @Override
    public void noMore() {

    }

    public void updateParamMap(){
        params.put("innerTableIdentifier",innerTableIdentifier.getGlobalIdentity());
        params.put("innerTableIndex",Integer.toString(innerTableIndex));
        params.put("outerTableIndex",Integer.toString(outerTableIndex));
        params.put("isCurrentTableInner", Boolean.toString(isCurrentTableInner));
        params.put("isInnerTableFinished",Boolean.toString(isInnerTableFinished));
    }

    @Override
    public void initialize() {
        innerTableHashMap = new HashMap<>();
        updateParamMap();
    }

    @Override
    public String getParam(String query) throws Exception {
        return params.getOrDefault(query,null);
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null && currentEntry.hasNext();
    }

    @Override
    public Tuple next() {
        return new AmberTuple(ArrayUtils.addAll(currentTuple,currentEntry.next()));
    }

    @Override
    public void dispose() {
        innerTableHashMap = null;
        currentEntry = null;
        currentTuple = null;
    }
}