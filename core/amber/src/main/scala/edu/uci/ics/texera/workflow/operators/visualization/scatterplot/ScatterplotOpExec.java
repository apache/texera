package edu.uci.ics.texera.workflow.operators.visualization.scatterplot;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
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

    public ScatterplotOpExec(ScatterplotOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        List<Object> resultObjects = new ArrayList<>();

        Schema resultSchema;
        if(opDesc.isGeometric)
            resultSchema = Schema.newBuilder().add(
                    new Attribute("longitude", t.getSchema().getAttribute(opDesc.xColumn).getType()),
                    new Attribute("latitude", t.getSchema().getAttribute(opDesc.yColumn).getType())
            ).build();

        else
            resultSchema = Schema.newBuilder().add(
                    new Attribute(opDesc.xColumn, t.getSchema().getAttribute(opDesc.xColumn).getType()),
                    new Attribute(opDesc.yColumn, t.getSchema().getAttribute(opDesc.yColumn).getType())
            ).build();

        resultObjects.add(t.getField(opDesc.xColumn));
        resultObjects.add(t.getField(opDesc.yColumn));
        return Tuple.newBuilder().add(resultSchema.getAttributes(), resultObjects).build();
    }
}