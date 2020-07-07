package edu.uci.ics.texera.api.engine;

import edu.uci.ics.texera.api.dataflow.ISink;
import java.util.HashMap;

public class MultipleSinkPlan extends Plan {

    private final HashMap<String, ISink> sinkMap;

    public MultipleSinkPlan(HashMap<String, ISink> sinkMap) {
        super();
        this.sinkMap = sinkMap;
    }

    public HashMap<String, ISink> getSinkMap() {
        return sinkMap;
    }
}
