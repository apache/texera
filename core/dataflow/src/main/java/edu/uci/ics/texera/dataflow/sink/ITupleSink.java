package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.List;

public interface ITupleSink extends ISink {
    void setInputOperator(IOperator inputOperator);

    List<Tuple> collectAllTuples();

    Schema transformToOutputSchema(Schema... inputSchema);
}
