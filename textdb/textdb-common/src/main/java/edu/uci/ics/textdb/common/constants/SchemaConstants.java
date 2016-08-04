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
    public static final String SPAN_LIST = "spanList";
    public static final Attribute SPAN_LIST_ATTRIBUTE = new Attribute(SPAN_LIST, FieldType.LIST);
    
    public static final String TERM_VECTOR = "termVector";
    public static final Attribute TERM_VECTOR_ATTRIBUTE = new Attribute(TERM_VECTOR, FieldType.LIST);
}
