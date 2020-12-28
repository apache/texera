package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import scala.Function1;

import java.io.Serializable;

public class TypeCastingOpExec extends  MapOpExec{
    private final TypeCastingOpDesc opDesc;
    public TypeCastingOpExec(TypeCastingOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        String attribute = opDesc.attribute;
        TypeCastingAttributeType resultType = opDesc.resultType;
        Class type = t.getField(attribute).getClass();

        if (type == String.class) {
            switch (resultType) {
                case STRING:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.STRING, t.getField(attribute)).build();
                case BOOLEAN:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.BOOLEAN, Boolean.parseBoolean(t.getField(attribute))).build();
                case DOUBLE:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.DOUBLE, Double.parseDouble(t.getField(attribute))).build();
                case INTEGER:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.INTEGER, Integer.parseInt(t.getField(attribute))).build();
            }
        } else if(type == Integer.class) {

            switch (resultType) {
                case STRING:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.STRING, Integer.toString(t.getField(attribute))).build();
                case BOOLEAN:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.BOOLEAN, ((Integer)t.getField(attribute))!=0 ).build();
                case DOUBLE:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.DOUBLE, new Double((Integer)t.getField(attribute))).build();
                case INTEGER:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.INTEGER, t.getField(attribute)).build();
            }
        } else if(type == Double.class) {
            switch (resultType) {
                case STRING:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.STRING, Double.toString(t.getField(attribute))).build();
                case BOOLEAN:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.BOOLEAN, ((Double)t.getField(attribute))!=0 ).build();
                case DOUBLE:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.DOUBLE,  t.getField(attribute)).build();
                case INTEGER:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.INTEGER, new Integer(((Double)t.getField(attribute)).intValue())).build();
            }
        } else if(type == Boolean.class) {
            switch (resultType) {
                case STRING:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.STRING, Boolean.toString(t.getField(attribute))).build();
                case BOOLEAN:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.BOOLEAN, t.getField(attribute)).build();
                case DOUBLE:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.DOUBLE, new Double( t.getField(attribute))).build();
                case INTEGER:
                    return Tuple.newBuilder().add(t).removeIfExists(attribute).add(attribute, AttributeType.INTEGER, new Integer( t.getField(attribute))).build();
            }

        }

        return t;
    }
}
