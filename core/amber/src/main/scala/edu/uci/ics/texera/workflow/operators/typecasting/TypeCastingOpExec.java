package edu.uci.ics.texera.workflow.operators.typecasting;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import edu.uci.ics.texera.workflow.common.tuple.schema.Attribute;
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType;
import scala.Function1;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class TypeCastingOpExec extends  MapOpExec{
    private final TypeCastingOpDesc opDesc;
    public TypeCastingOpExec(TypeCastingOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        String attribute = opDesc.attribute;
        TypeCastingAttributeType resultType = opDesc.resultType;
        List<Attribute> attributes = t.getSchema().getAttributes();
        List<String> attributeNames = t.getSchema().getAttributeNames();
        List<AttributeType> attributeTypes = attributes.stream().map(attr -> attr.getType()).collect(toList());
        List<Object> fields = t.getFields();
        Tuple.Builder builder = Tuple.newBuilder();
        for (int i=0; i<attributeNames.size();i++) {
            if (attributeNames.get(i).equals(attribute)) {
                builder = casting(builder, attribute, attributeTypes.get(i), resultType, t);
            } else {
                builder.add(attributeNames.get(i), attributeTypes.get(i), fields.get(i));
            }
        }


        return builder.build();
    }
    private Tuple.Builder casting(Tuple.Builder builder, String attribute, AttributeType type, TypeCastingAttributeType resultType,  Tuple t) {
        if (type == AttributeType.STRING) {
            switch (resultType) {
                case STRING:
                    return builder.add(attribute, AttributeType.STRING, t.getField(attribute));
                case BOOLEAN:
                    return builder.add(attribute, AttributeType.BOOLEAN, Boolean.parseBoolean(t.getField(attribute)));
                case DOUBLE:
                    return builder.add(attribute, AttributeType.DOUBLE, Double.parseDouble(t.getField(attribute)));
                case INTEGER:
                    return builder.add(attribute, AttributeType.INTEGER, Integer.parseInt(t.getField(attribute)));
            }
        } else if(type == AttributeType.INTEGER) {

            switch (resultType) {
                case STRING:
                    return builder.add(attribute, AttributeType.STRING, Integer.toString(t.getField(attribute)));
                case BOOLEAN:
                    return builder.add(attribute, AttributeType.BOOLEAN, ((Integer)t.getField(attribute))!=0 );
                case DOUBLE:
                    return builder.add(attribute, AttributeType.DOUBLE, new Double((Integer)t.getField(attribute)));
                case INTEGER:
                    return builder.add(attribute, AttributeType.INTEGER, t.getField(attribute));
            }
        } else if(type == AttributeType.DOUBLE) {
            switch (resultType) {
                case STRING:
                    return builder.add(attribute, AttributeType.STRING, Double.toString(t.getField(attribute)));
                case BOOLEAN:
                    return builder.add(attribute, AttributeType.BOOLEAN, ((Double)t.getField(attribute))!=0 );
                case DOUBLE:
                    return builder.add(attribute, AttributeType.DOUBLE,  t.getField(attribute));
                case INTEGER:
                    return builder.add(attribute, AttributeType.INTEGER, new Integer(((Double)t.getField(attribute)).intValue()));
            }
        } else if(type == AttributeType.BOOLEAN) {
            switch (resultType) {
                case STRING:
                    return builder.add(attribute, AttributeType.STRING, Boolean.toString(t.getField(attribute)));
                case BOOLEAN:
                    return builder.add(attribute, AttributeType.BOOLEAN, t.getField(attribute));
                case DOUBLE:
                    return builder.add(attribute, AttributeType.DOUBLE, new Double( t.getField(attribute)));
                case INTEGER:
                    return builder.add(attribute, AttributeType.INTEGER, new Integer( t.getField(attribute)));
            }

        }
        return builder;
    }
}
