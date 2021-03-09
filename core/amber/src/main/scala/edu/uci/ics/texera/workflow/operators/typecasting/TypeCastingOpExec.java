package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.Function1;

import java.io.Serializable;


public class TypeCastingOpExec extends  MapOpExec{
    private final TypeCastingOpDesc opDesc;
    public TypeCastingOpExec(TypeCastingOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        return AttributeTypeUtils.TupleCasting(t, opDesc.attribute, opDesc.resultType);
    }

}
