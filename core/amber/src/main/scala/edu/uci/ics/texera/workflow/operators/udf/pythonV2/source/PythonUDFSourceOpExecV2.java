package edu.uci.ics.texera.workflow.operators.udf.pythonV2.source;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2;
import scala.collection.Iterator;
import scala.util.Either;

public class PythonUDFSourceOpExecV2 extends PythonUDFOpExecV2 implements SourceOperatorExecutor {


    public PythonUDFSourceOpExecV2(String code, Schema outputSchema) {
        super(code, outputSchema);
    }


    @Override
    public Iterator<ITuple> processTuple(Either<ITuple, InputExhausted> tuple, LinkIdentity input) {
        return SourceOperatorExecutor.super.processTuple(tuple, input);
        // Will not be used. The real implementation is in the Python UDF.
    }

    @Override
    public Iterator<ITuple> produce() {
        // Will not be used. The real implementation is in the Python UDF.
        return SourceOperatorExecutor.super.produce();
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        // Will not be used. The real implementation is in the Python UDF.
        return null;
    }
}
