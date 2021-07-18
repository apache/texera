package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils;
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema;
import scala.Function1;

import java.io.Serializable;


public class TypeCastingOpExec extends MapOpExec {

    private final Schema castToSchema;

    public TypeCastingOpExec(Schema castToSchema) {
        this.castToSchema = castToSchema;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple tuple) {
        return AttributeTypeUtils.TupleCasting(tuple, castToSchema);
    }

}
