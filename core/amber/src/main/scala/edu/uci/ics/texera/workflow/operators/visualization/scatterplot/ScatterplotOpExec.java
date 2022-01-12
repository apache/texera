package edu.uci.ics.texera.workflow.operators.visualization.scatterplot;

import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.amber.engine.common.InputExhausted;
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity;
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.util.*;


/**
 * Scatterplot operator to visualize the result as a scatterplot
 */
public class ScatterplotOpExec implements OperatorExecutor {
    private final ScatterplotOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;
    private boolean[][] pixelGrid;
    private List<Tuple> result;

    public ScatterplotOpExec(ScatterplotOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
    }

    private List<Tuple> processTuple(Tuple t) {
        result.clear();

        if (opDesc.isGeometric) {
            int row_index = (int) Math.floor(lngX(t.getField(opDesc.xColumn)) * Constants.MAX_RESOLUTION_ROWS());
            int column_index = (int) Math.floor(latY(t.getField(opDesc.yColumn)) * Constants.MAX_RESOLUTION_COLUMNS());
            if (pixelGrid[row_index][column_index]) {
                return result;
            }
            pixelGrid[row_index][column_index] = true;
        }
        Object[] resultObjects = new Object[2];
        resultObjects[0] = t.getField(opDesc.xColumn);
        resultObjects[1] = t.getField(opDesc.yColumn);
        result.add(Tuple.newBuilder(operatorSchemaInfo.outputSchema()).addSequentially(resultObjects).build());
        return result;
    }

    // longitude to spherical mercator in [0..1] range
    public static double lngX(double lng) {
        return lng / 360 + 0.5;
    }

    // latitude to spherical mercator in [0..1] range
    public static double latY(double lat) {
        double sin = Math.sin(lat * Math.PI / 180);
        double y = (0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI);
        return y < 0 ? 0 : y > 1 ? 1 : y;
    }

    @Override
    public void open() {
        result = new ArrayList<>();
        if (opDesc.isGeometric) {
            pixelGrid = new boolean[Constants.MAX_RESOLUTION_ROWS()][Constants.MAX_RESOLUTION_COLUMNS()];
        }
    }

    @Override
    public void close() {
        pixelGrid = null;
        result = null;
    }

    @Override
    public Iterator<Tuple> processTexeraTuple(Either<Tuple, InputExhausted> tuple, LinkIdentity input) {
        if (tuple.isLeft()) {
            return JavaConverters.asScalaIterator(processTuple(tuple.left().get()).iterator());
        } else { // input exhausted
            return JavaConverters.asScalaIterator(Collections.emptyIterator());
        }
    }
}