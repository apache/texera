package edu.uci.ics.textdb.perftest.promed;

import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;

public class PromedSchema {
    
    public static final String ID = "id";
    public static final String CONTENT = "content";
    
    public static final Attribute ID_ATTR = new Attribute(ID, AttributeType.STRING);
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, AttributeType.TEXT);
    
    public static final Schema PROMED_SCHEMA = new Schema(ID_ATTR, CONTENT_ATTR);

}
