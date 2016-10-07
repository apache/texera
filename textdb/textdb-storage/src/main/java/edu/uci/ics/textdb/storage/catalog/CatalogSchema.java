package edu.uci.ics.textdb.storage.catalog;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.Schema;

public class CatalogSchema {
    
    // Schema for the "document catalog" Document
 static final String DOCUMENT_ID = "documentID";
    public static final String DOCUMENT_NAME = "documentName";
    public static final String DOCUMENT_DIRECTORY = "documentDirectory";
    public static final String DOCUMENT_LUCENE_ANALYZER = "luceneAnalyzer";
    
    public static final Attribute DOCUMENT_ID_ATTR = new Attribute(DOCUMENT_ID, FieldType.INTEGER);
    public static final Attribute DOCUMENT_NAME_ATTR = new Attribute(DOCUMENT_NAME, FieldType.STRING);
    public static final Attribute DOCUMENT_DIRECTORY_ATTR = new Attribute(DOCUMENT_DIRECTORY, FieldType.STRING);
    public static final Attribute DOCUMENT_LUCENE_ANALYZER_ATTR = new Attribute(DOCUMENT_LUCENE_ANALYZER, FieldType.STRING);
    
    public static final Schema CATALOG_DOCUMENT_SCHEMA = new Schema(
            DOCUMENT_ID_ATTR, DOCUMENT_NAME_ATTR, DOCUMENT_DIRECTORY_ATTR); 
    
    
    // Schema for "schema catalog" Document
    public static final String ATTR_NAME = "attributeName";
    public static final String ATTR_TYPE = "attributeType";
    
    public static final Attribute ATTR_NAME_ATTR = new Attribute(ATTR_NAME, FieldType.STRING);
    public static final Attribute ATTR_TYPE_ATTR = new Attribute(ATTR_TYPE, FieldType.STRING);
    
    public static final Schema CATALOG_SCHEMA_SCHEMA = new Schema(
            DOCUMENT_ID_ATTR, ATTR_NAME_ATTR, ATTR_TYPE_ATTR);

}
