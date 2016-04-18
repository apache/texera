/**
 * 
 */
package edu.uci.ics.textdb.common.constants;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;

/**
 * @author sandeepreddy602
 *
 */
public class SchemaConstants {
    public static final String SPAN_BEGIN = "spanBegin";
    public static final String SPAN_END = "spanEnd";
    public static final String SPAN_FIELD_NAME = "spanFieldName";
    public static final String SPAN_KEY = "spanKey";
    
    public static final Attribute SPAN_BEGIN_ATTRIBUTE = new Attribute(SPAN_BEGIN, FieldType.INTEGER);
    public static final Attribute SPAN_END_ATTRIBUTE = new Attribute(SPAN_END, FieldType.INTEGER);
    public static final Attribute SPAN_FIELD_NAME_ATTRIBUTE = new Attribute(SPAN_FIELD_NAME, FieldType.STRING);
    public static final Attribute SPAN_KEY_ATTRIBUTE = new Attribute(SPAN_KEY, FieldType.STRING);
}
