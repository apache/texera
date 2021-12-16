package edu.uci.ics.texera.workflow.operators.visualization.distributedBarChart;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;
import scala.Serializable;

public class DistributedBarChartExec extends MapOpExec {

    private final DistributedBarChartDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public DistributedBarChartExec(DistributedBarChartDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        Tuple.BuilderV2 builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema());
        Schema inputSchema = t.getSchema();

        builder.add(inputSchema.getAttribute(opDesc.nameColumn()), t.getField(opDesc.nameColumn()));

        for(int i = 0; i < opDesc.resultAttributeNames().length(); i++) {
            String dataName = opDesc.resultAttributeNames().apply(i);
            builder.add(inputSchema.getAttribute(dataName), t.getField(dataName));
        }

        return builder.build();
    }
}
