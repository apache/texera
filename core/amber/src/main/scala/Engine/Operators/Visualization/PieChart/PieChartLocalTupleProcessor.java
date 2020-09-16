package Engine.Operators.Visualization.PieChart;

import Engine.Common.AmberTag.LayerTag;
import Engine.Common.AmberTuple.Tuple;
import Engine.Common.TupleProcessor;

import java.util.*;

public class PieChartLocalTupleProcessor implements TupleProcessor {
    private final int nameColumn;
    private final int dataColumn;
    private List<Tuple> result;
    private Iterator<Tuple> iterator = null;

    public PieChartLocalTupleProcessor(int nameColumn, int dataColumn) {
        this.nameColumn = nameColumn;
        this.dataColumn = dataColumn;
    }

    @Override
    public void accept(Tuple tuple) throws Exception {
        // TODO: check type after schema is introduced.
        String name = tuple.getString(nameColumn);
        Object o = tuple.get(dataColumn);
        double data;
        if (o.getClass() != Double.class && o.getClass() != Integer.class) {
            if (o.getClass() == String.class) {
                data = Double.parseDouble((String) o);
            }
            else throw new RuntimeException("bar chart: data column is not number type");
        }
        else {
            assert o instanceof Double;
            data = (Double) o;
        }

        result.add(Tuple.fromJavaList(Arrays.asList(name, data)));
    }

    @Override
    public void onUpstreamChanged(LayerTag from) {

    }

    @Override
    public void onUpstreamExhausted(LayerTag from) {

    }

    @Override
    public void noMore() {
        result.sort((left, right) -> {
            double leftValue = left.getDouble(1);
            double rightValue = right.getDouble(1);
            return Double.compare(rightValue, leftValue);
        });
        iterator = result.iterator();
    }

    @Override
    public void initialize() throws Exception {
        result = new ArrayList<>();
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
        result = null;
    }
}
