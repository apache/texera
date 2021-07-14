package edu.uci.ics.texera.workflow.operators.visualization.pieChart;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Sort all the tuples and replace the ones whose numbers make up very little to the sum (below pruneRatio) with
 * a new tuple (others).
 *
 * @author Mingji Han, Xiaozhen Liu
 */
public class PieChartOpFinalExec implements OperatorExecutor {
    private final Double pruneRatio;
    private List<Tuple> tempList;
    private List<Tuple> resultList;
    private double sum = 0.0;
    private Schema resultSchema = null;

    public PieChartOpFinalExec(Double pruneRatio) {
        this.pruneRatio = pruneRatio;
    }

    @Override
    public void open() {
        tempList = new ArrayList<>();
        resultList = new ArrayList<>();
    }

    @Override
    public void close() {

    }

    @Override
    public String getParam(String query) {
        return null;
    }

    @Override
    public scala.collection.Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            sum += tuple.left().get().getDouble(1);
            tempList.add(tuple.left().get());
            if (resultSchema == null) resultSchema = tuple.left().get().getSchema();
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        } else {
            // sort all tuples in descending order
            tempList.sort((left, right) -> {
                double leftValue = left.getDouble(1);
                double rightValue = right.getDouble(1);
                return Double.compare(rightValue, leftValue);
            });

            // process the sorted rows, if the cumulative sum is greater than ratio * sum.
            // stop adding tuples, add new row called "Other" instead.
            double total = 0.0;
            for (Tuple t : tempList) {
                total += t.getDouble(1);
                resultList.add(t);
                if (total / sum > pruneRatio) {
                    double otherDataField = sum - total;
                    resultList.add(Tuple.newBuilder(resultSchema).addSequentially(new Object[]{"Other", otherDataField}).build());
                    break;
                }
            }
            return JavaConverters.asScalaIterator(resultList.iterator());
        }


    }
}
