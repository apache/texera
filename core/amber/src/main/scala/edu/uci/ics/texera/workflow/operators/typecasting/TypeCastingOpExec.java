package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo;
import scala.Function1;

import java.io.Serializable;


public class TypeCastingOpExec extends  MapOpExec{
    private final TypeCastingOpDesc opDesc;
    private final OperatorSchemaInfo operatorSchemaInfo;

    public TypeCastingOpExec(TypeCastingOpDesc opDesc, OperatorSchemaInfo operatorSchemaInfo) {
        this.opDesc = opDesc;
        this.operatorSchemaInfo = operatorSchemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        return AttributeTypeUtils.TupleCasting(t, opDesc.attribute, opDesc.resultType, operatorSchemaInfo);
    }

}
