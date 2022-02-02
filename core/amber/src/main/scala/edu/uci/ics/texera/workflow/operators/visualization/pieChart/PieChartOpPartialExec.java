package edu.uci.ics.texera.workflow.operators.visualization.pieChart;

import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.*;

/**
 * Simply extract relevant fields and do partial sorting.
 * @author Mingji Han, Xiaozhen Liu
 */
public class PieChartOpPartialExec implements OperatorExecutor {
    private final String nameColumn;
    private final String dataColumn;
    private boolean noDataCol;
    private List<Tuple> result;

    public PieChartOpPartialExec(String nameColumn, String dataColumn) {
        this.nameColumn = nameColumn;
        this.noDataCol = dataColumn == null || dataColumn.equals("");
        this.dataColumn = noDataCol? "count": dataColumn;
    }

    @Override
    public void open() {
        result = new ArrayList<>();
    }

    @Override
    public void close() {
    }

    @Override
    public String getParam(String query) {
        return null;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            Tuple inputTuple = tuple.left().get();
            String name = inputTuple.getField(nameColumn);
            Double data;
            if (inputTuple.getSchema().getAttribute(dataColumn).getType() == AttributeType.STRING) {
                data = Double.parseDouble(inputTuple.getField(dataColumn));
            } else if (inputTuple.getSchema().getAttribute(dataColumn).getType() == AttributeType.INTEGER) {
                data = Double.parseDouble(Integer.toString(inputTuple.getField(dataColumn)));
            } else {
                data = inputTuple.getField(dataColumn);
            }
            Schema oldSchema = tuple.left().get().getSchema();
            Attribute dataAttribute = new Attribute(oldSchema.getAttribute(dataColumn).getName(), oldSchema.getAttribute(dataColumn).getType());
            Schema newSchema = new Schema(Arrays.asList(oldSchema.getAttribute(nameColumn), dataAttribute));
            if (noDataCol) {
                result.add(Tuple.newBuilder(newSchema).addSequentially(new Object[]{name, data.intValue()}).build());
            } else {
                result.add(Tuple.newBuilder(newSchema).addSequentially(new Object[]{name, data}).build());
            }
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        }
        else {
            result.sort((left, right) -> {
                double leftValue;
                double rightValue;
                if (noDataCol) {
                    leftValue = left.getInt(1);
                    rightValue = right.getInt(1);
                } else {
                    leftValue = left.getDouble(1);
                    rightValue = right.getDouble(1);
                }
                return Double.compare(rightValue, leftValue);
            });
            return JavaConverters.asScalaIterator(result.iterator());
        }
    }
}
