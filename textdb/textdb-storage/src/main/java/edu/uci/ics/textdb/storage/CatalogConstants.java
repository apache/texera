package edu.uci.ics.textdb.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * CatalogConstants stores the schema and the initial tuples of the catalog manager
 * 
 * 
 * ============================================
 * 
 * Initial tuples for the table catalog:
 * 
 *  tableName    |    tableDirectory    |    luceneAnalyzer
 * 
 * tableCatalog       ../catalog/table       standardLuceneAnalyzer
 * schemaCatalog      ../catalog/schema      standardLuceneAnalyzer
 *   
 * ============================================
 *   
 *  Initial tuples for the schema catalog. 
 *    tableName    |    attributeName    |  attributeType  |  attributePosition
 *  
 *   tableCatalog       tableName           string                0
 *   tableCatalog    tableDirectory         string                1
 *   tableCatalog     luceneAnalyzer        string                2
 *   schemaCatalog      tableName           string                0
 *   schemaCatalog    attributeName         string                1
 *   schemaCatalog    attributeType         string                2
 *   schemaCatalog  attributePosition       string                3
 *     
 * ============================================
 * 
 * @author Zuozhi Wang
 *
 */
public class CatalogConstants {

    public static final String TABLE_CATALOG = "tableCatalog";
    public static final String SCHEMA_CATALOG = "schemaCatalog";

    public static final String TABLE_CATALOG_DIRECTORY = "../catalog/table";
    public static final String SCHEMA_CATALOG_DIRECTORY = "../catalog/schema";

    // Schema for the "table catalog" table
    public static final String TABLE_NAME = "tableName";
    public static final String TABLE_DIRECTORY = "tableDirectory";
    public static final String TABLE_LUCENE_ANALYZER = "luceneAnalyzer";

    public static final Attribute TABLE_NAME_ATTR = new Attribute(TABLE_NAME, FieldType.STRING);
    public static final Attribute TABLE_DIRECTORY_ATTR = new Attribute(TABLE_DIRECTORY, FieldType.STRING);
    public static final Attribute TABLE_LUCENE_ANALYZER_ATTR = new Attribute(TABLE_LUCENE_ANALYZER,
            FieldType.STRING);

    public static final Schema TABLE_CATALOG_SCHEMA = new Schema(TABLE_NAME_ATTR, TABLE_DIRECTORY_ATTR,
            TABLE_LUCENE_ANALYZER_ATTR);
    public static final Schema TABLE_CATALOG_SCHEMA_WITH_ID = Utils.getSchemaWithID(TABLE_CATALOG_SCHEMA);

    // Schema for "schema catalog" table
    public static final String ATTR_NAME = "attributeName";
    public static final String ATTR_TYPE = "attributeType";
    public static final String ATTR_POSITION = "attributePosition";

    public static final Attribute ATTR_NAME_ATTR = new Attribute(ATTR_NAME, FieldType.STRING);
    public static final Attribute ATTR_TYPE_ATTR = new Attribute(ATTR_TYPE, FieldType.STRING);
    public static final Attribute ATTR_POSITION_ATTR = new Attribute(ATTR_POSITION, FieldType.INTEGER);

    public static final Schema SCHEMA_CATALOG_SCHEMA = new Schema(TABLE_NAME_ATTR, ATTR_NAME_ATTR, ATTR_TYPE_ATTR,
            ATTR_POSITION_ATTR);
    public static final Schema SCHEMA_CATALOG_SCHEMA_WITH_ID = Utils.getSchemaWithID(SCHEMA_CATALOG_SCHEMA);

    
    // DataStore for table catalog and schema catalog
    public static final DataStore TABLE_CATALOG_DATASTORE = 
            new DataStore(TABLE_CATALOG_DIRECTORY, TABLE_CATALOG_SCHEMA_WITH_ID);
    public static final DataStore SCHEMA_CATALOG_DATASTORE = 
            new DataStore(SCHEMA_CATALOG_DIRECTORY, SCHEMA_CATALOG_SCHEMA_WITH_ID);


    /**
     * Gets the tuple to be inserted to the table catalog.
     * 
     * @param tableName
     * @param tableDirectory
     * @param luceneAnalyzerStr
     * @return
     * @throws StorageException
     */
    public static ITuple getTableCatalogTuple(String tableName, String tableDirectory, String luceneAnalyzerStr) 
            throws StorageException {
        try {
            String tableDirectoryAbsolute = new File(tableDirectory).getCanonicalPath();
            return new DataTuple(TABLE_CATALOG_SCHEMA, 
                    new StringField(tableName), 
                    new StringField(tableDirectoryAbsolute),
                    new StringField(luceneAnalyzerStr));
        } catch (IOException e) {
            throw new StorageException(String.format("Error occurs when getting the canonical path of %s.", tableDirectory));
        }

    }
    
    /**
     * Gets the tuples to be inserted to the schema catalog.
     * 
     * @param tableName
     * @param tableDirectory
     * @param luceneAnalyzerStr
     * @return
     * @throws StorageException
     */
    public static List<ITuple> getSchemaCatalogTuples(String tableName, Schema tableSchema) {
        List<ITuple> schemaCatalogTuples = new ArrayList<>();
        for (int i = 0; i < tableSchema.getAttributes().size(); i++) {
            Attribute attr = tableSchema.getAttributes().get(i);
            ITuple schemaTuple = new DataTuple(SCHEMA_CATALOG_SCHEMA, 
                    new StringField(tableName),
                    new StringField(attr.getFieldName()),
                    new StringField(attr.getFieldType().toString().toLowerCase()),
                    new IntegerField(i));
            schemaCatalogTuples.add(schemaTuple);
        }
        return schemaCatalogTuples;
    }

}
