package edu.uci.ics.texera.workflow.operators.visualization.scatterplot;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;
import scala.Serializable;
import java.util.*;


/**
 * Scatterplot operator to visualize the result as a scatterplot
 *
 */
public class ScatterplotOpExec extends MapOpExec {
    private final ScatterplotOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public ScatterplotOpExec(ScatterplotOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        Object[] resultObjects = new Object[2];

        resultObjects[0] = t.getField(opDesc.xColumn);
        resultObjects[1] = t.getField(opDesc.yColumn);

        return Tuple.newBuilder(operatorSchemaInfo.outputSchema()).addSequentially(resultObjects).build();
    }
}