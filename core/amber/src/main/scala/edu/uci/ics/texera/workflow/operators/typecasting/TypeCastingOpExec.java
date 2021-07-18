package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import scala.Function1;

import java.io.Serializable;


public class TypeCastingOpExec extends MapOpExec {

    private final OperatorSchemaInfo operatorSchemaInfo;

    public TypeCastingOpExec(OperatorSchemaInfo operatorSchemaInfo) {
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple tuple) {
        return AttributeTypeUtils.TupleCasting(tuple, operatorSchemaInfo.outputSchema());
    }

}
