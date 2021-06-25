package edu.uci.ics.texera.api.constants;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;

/**
 * @author Zuozhi Wang
 * @author sandeepreddy602
 *
 */
public class SchemaConstants {
    public static final String PAYLOAD = "payload";
    public static final Attribute PAYLOAD_ATTRIBUTE = new Attribute(PAYLOAD, AttributeType.LIST);

    public static final String SPAN_LIST = "spanList";
    public static final Attribute SPAN_LIST_ATTRIBUTE = new Attribute(SPAN_LIST, AttributeType.LIST);
    
    public static final String _ID = "_id";
    public static final Attribute _ID_ATTRIBUTE = new Attribute(_ID, AttributeType._ID_TYPE);
}
