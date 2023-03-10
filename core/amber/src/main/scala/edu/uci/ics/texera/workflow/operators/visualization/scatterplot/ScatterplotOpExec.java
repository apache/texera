package edu.uci.ics.texera.workflow.operators.visualization.scatterplot;

import edu.uci.ics.amber.engine.common.Constants;
import edu.uci.ics.texera.workflow.common.operators.flatmap.FlatMapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Scatterplot operator to visualize the result as a scatterplot
 */
public class ScatterplotOpExec extends FlatMapOpExec {
    private final ScatterplotOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;
    private boolean[][] pixelGrid;
    private List<Tuple> result;

    public ScatterplotOpExec(ScatterplotOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setFlatMapFuncJava(this::process);
    }

    private Iterator<Tuple> process(Tuple t) {
        result.clear();

        if (opDesc.isGeometric) {
            int row_index = (int) Math.floor(lngX(t.getField(opDesc.xColumn)) * Constants.MAX_RESOLUTION_ROWS());
            int column_index = (int) Math.floor(latY(t.getField(opDesc.yColumn)) * Constants.MAX_RESOLUTION_COLUMNS());
            if (pixelGrid[row_index][column_index]) {
                return result.iterator();
            }
            pixelGrid[row_index][column_index] = true;
        }
        Object[] resultObjects = new Object[2];
        resultObjects[0] = t.getField(opDesc.xColumn);
        resultObjects[1] = t.getField(opDesc.yColumn);
        result.add(Tuple.newBuilder(operatorSchemaInfo.outputSchemas()[0]).addSequentially(resultObjects).build());
        return result.iterator();
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

}