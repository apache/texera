package edu.uci.ics.texera.workflow.operators.udf.python.source;

import edu.uci.ics.amber.engine.architecture.worker.PauseManager;
import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient;
import edu.uci.ics.amber.engine.common.tuple.ITuple;
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.util.Either;

public class PythonUDFSourceOpExecV2 extends PythonUDFOpExecV2 implements SourceOperatorExecutor {


    public PythonUDFSourceOpExecV2(String code, Schema outputSchema) {
        super(code, outputSchema);
    }


    @Override
    public Iterator<Tuple2<ITuple, Option<Object>>> processTuple(Either<ITuple, InputExhausted> tuple, int input, PauseManager pauseManager, AsyncRPCClient asyncRPCClient) {
        return SourceOperatorExecutor.super.processTuple(tuple, input, pauseManager, asyncRPCClient);
        // Will not be used. The real implementation is in the Python UDF.
    }

    @Override
    public Iterator<Tuple> produceTexeraTuple() {
        // Will not be used. The real implementation is in the Python UDF.
        return null;
    }
}
