package edu.uci.ics.textdb.api.field;

import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.uci.ics.textdb.api.constants.JsonConstants;
import edu.uci.ics.textdb.api.schema.AttributeType;

/**
 * Created by chenli on 3/31/16.
 */
public interface IField {
    
    @JsonProperty(value = JsonConstants.FIELD_VALUE)
    Object getValue();
    
    public static HashMap<Class<? extends IField>, AttributeType> fieldTypeMap = new HashMap<Class<? extends IField>, AttributeType>() {
        private static final long serialVersionUID = 7780232078216757034L; 
    {
        put(DateField.class, AttributeType.DATE);
        put(DoubleField.class, AttributeType.DOUBLE);
        put(IDField.class, AttributeType._ID_TYPE);
        put(IntegerField.class, AttributeType.INTEGER);
        put(ListField.class, AttributeType.LIST);
        put(StringField.class, AttributeType.STRING);
        put(TextField.class, AttributeType.TEXT);
    }
    };
    
    
}
