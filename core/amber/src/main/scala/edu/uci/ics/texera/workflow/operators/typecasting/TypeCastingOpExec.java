package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.SchemaInfo;
import scala.Function1;

import java.io.Serializable;


public class TypeCastingOpExec extends  MapOpExec{
    private final TypeCastingOpDesc opDesc;
    private final SchemaInfo schemaInfo;
    public TypeCastingOpExec(TypeCastingOpDesc opDesc, SchemaInfo schemaInfo) {
        this.opDesc = opDesc;
        this.schemaInfo = schemaInfo;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        return AttributeTypeUtils.TupleCasting(t, schemaInfo.outputSchema(), opDesc.attribute, opDesc.resultType);
    }

}
