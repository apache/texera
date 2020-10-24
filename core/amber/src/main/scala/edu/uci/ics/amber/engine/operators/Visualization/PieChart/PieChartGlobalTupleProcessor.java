package Engine.Operators.Visualization.PieChart;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PieChartGlobalTupleProcessor implements TupleProcessor {
    private final Double pruneRatio;
    private List<Tuple> tempList;
    private List<Tuple> resultList;
    private Iterator<Tuple> iterator = null;
    private double sum = 0.0;

    public PieChartGlobalTupleProcessor(Double pruneRatio) {
        this.pruneRatio = pruneRatio;
    }

    @Override
    public void accept(Tuple tuple) throws Exception {
        sum += tuple.getDouble(1);
        tempList.add(tuple);
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {
        // sort all tuples in descending order
        tempList.sort((left, right) -> {
            double leftValue = left.getDouble(1);
            double rightValue = right.getDouble(1);
            return Double.compare(rightValue, leftValue);
        });

        // process the sorted rows, if the cumulative sum is greater than ratio * sum.
        // stop adding tuples, add new row called "Other" instead.
        double total = 0.0;
        for (Tuple t: tempList) {
            total += t.getDouble(1);
            resultList.add(t);
            if (total / sum > pruneRatio) {
                String otherNameField = "Other";
                Double otherDataField = sum - total;
                resultList.add(Tuple.fromJavaList(Arrays.asList(otherNameField, otherDataField)));
                break;
            }
        }
        iterator = resultList.iterator();
    }

    @Override
    public void initialize() throws Exception {
        tempList = new ArrayList<>();
        resultList = new ArrayList<>();
    }

    @Override
    public boolean hasNext() throws Exception {
        return iterator != null && iterator.hasNext();
    }

    @Override
    public Tuple next() throws Exception {
        return iterator.next();
    }

    @Override
    public void dispose() throws Exception {

    }
}
