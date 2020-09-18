package Engine.SchemaSupport.field;


import Engine.SchemaSupport.schema.AttributeType;

import java.text.ParseException;
import java.util.Arrays;

public class ParseUtils {
    public static IField getField(AttributeType attributeType, String fieldValue) throws ParseException {
        IField field = null;
        switch (attributeType) {
            case _ID_TYPE:
                field = new IDField(fieldValue);
                break;
            case STRING:
                field = new StringField(fieldValue);
                break;
            case INTEGER:
                field = new IntegerField(Integer.parseInt(fieldValue));
                break;
            case DOUBLE:
                field = new DoubleField(Double.parseDouble(fieldValue));
                break;
            case DATE:
                field = new DateField(fieldValue);
                break;
            case DATETIME:
                field = new DateTimeField(fieldValue);
                break;
            case TEXT:
                field = new TextField(fieldValue);
                break;
            case LIST:
                // LIST FIELD SHOULD BE CREATED ON ITS OWN
                // WARNING! This case should never be reached.
                field = new ListField<String>(Arrays.asList(fieldValue));
                break;
        }
        return field;
    }
}
