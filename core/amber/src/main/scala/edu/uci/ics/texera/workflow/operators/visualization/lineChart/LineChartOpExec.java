package edu.uci.ics.texera.workflow.operators.visualization.lineChart;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;
import scala.Serializable;

import java.util.ArrayList;
import java.util.List;

public class LineChartOpExec extends MapOpExec {

    private final LineChartOpDesc opDesc;

    public LineChartOpExec(LineChartOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        List<Object> resultObjects = new ArrayList<>();
        List<Attribute> resultAttributes = new ArrayList<>();

        Schema inputSchema = t.getSchema();

        resultObjects.add(t.getField(opDesc.nameColumn));
        resultAttributes.add(inputSchema.getAttribute(opDesc.nameColumn));

        for(String s : opDesc.dataColumns) {
            resultObjects.add(t.getField(s));
            resultAttributes.add(inputSchema.getAttribute(s));
        }
        return Tuple.newBuilder().add(resultAttributes, resultObjects).build();
    }
}
