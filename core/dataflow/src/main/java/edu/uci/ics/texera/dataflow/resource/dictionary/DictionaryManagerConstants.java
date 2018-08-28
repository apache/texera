package edu.uci.ics.texera.dataflow.resource.dictionary;

import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;

public class DictionaryManagerConstants {

    public static final String TABLE_NAME = "dictionary";

    public static final String NAME = "name";
    public static final Attribute NAME_ATTR = new Attribute(NAME, AttributeType.STRING);
    public static final String CONTENT = "content";
    public static final Attribute CONTENT_ATTR = new Attribute(CONTENT, AttributeType.STRING);
    
    public static final Schema SCHEMA = new Schema(NAME_ATTR, CONTENT_ATTR);
    
}
