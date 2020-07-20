package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.List;

public interface ITupleSink extends ISink {

    List<Tuple> collectAllTuples();


}
