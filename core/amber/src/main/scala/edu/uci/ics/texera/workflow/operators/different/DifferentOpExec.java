package edu.uci.ics.texera.workflow.operators.different;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.amber.error.WorkflowRuntimeError;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import scala.util.Either;

import java.util.HashSet;

public class DifferentOpExec implements OperatorExecutor {
    private boolean isRightTableFinished = false;
    private final LinkIdentity rightTable;
    private HashSet<Tuple> rightTableHashSet;
    private final HashSet<Tuple> resultHashSet;

    public DifferentOpExec(LinkIdentity rightTable) {
//        TODO: Clone or assign reference?
        this.rightTable = rightTable;
        this.resultHashSet = new HashSet<>();
    }

    @Override
//    TODO: Preserve order? Remove duplication?
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            if (input == rightTable) {
                rightTableHashSet.add(tuple.left().get());
            } else {
                if (isRightTableFinished) {
                    if (!rightTableHashSet.contains(tuple.left().get())) {
                        resultHashSet.add(tuple.left().get());
                    }
                } else {
                    StringBuilder sb = new StringBuilder();
                    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
                    for (int i = 0; i < stackTraceElements.length; ++i) {
                        sb.append(stackTraceElements[i].toString());
                        if (i < stackTraceElements.length - 1) {
                            sb.append("\n");
                        }
                    }
                    WorkflowRuntimeError err = new WorkflowRuntimeError("Left table came before right table ended",
                            "DifferentOpExec",
                            new Map.Map1<>("stacktrace", sb.toString()));
                    throw new WorkflowRuntimeException(err);
                }
            }
        } else {
            if (input == rightTable) {
                isRightTableFinished = true;
            }
        }
        return JavaConverters.asScalaIterator(resultHashSet.iterator());
    }

    @Override
    public void open() {
        rightTableHashSet = new HashSet<>();
    }

    @Override
    public void close() {
        rightTableHashSet.clear();
    }
}

