package edu.uci.ics.texera.workflow.operators.visualization.barChart;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;
import scala.Serializable;

import java.util.ArrayList;
import java.util.List;

public class BarChartOpExec extends MapOpExec {

    private final BarChartOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public BarChartOpExec(BarChartOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        Tuple.BuilderV2 builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema());
        Schema inputSchema = t.getSchema();

        builder.add(inputSchema.getAttribute(opDesc.nameColumn), t.getField(opDesc.nameColumn));

        for(String s : opDesc.dataColumns) {
            builder.add(inputSchema.getAttribute(s), t.getField(s));
        }

        return builder.build();
    }
}
