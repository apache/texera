package edu.uci.ics.textdb.storage.catalog;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.StringField;

public class CatalogConstants {

    public static final String COLLECTION_CATALOG = "collectionCatalog";
    public static final String SCHEMA_CATALOG = "schemaCatalog";

    public static final String COLLECTION_CATALOG_DIRECTORY = "../catalog/collection";
    public static final String SCHEMA_CATALOG_DIRECTORY = "../catalog/schema";

    // Schema for the "collection catalog" Collection
    public static final String COLLECTION_NAME = "collectionName";
    public static final String COLLECTION_DIRECTORY = "collectionDirectory";
    public static final String COLLECTION_LUCENE_ANALYZER = "luceneAnalyzer";

    public static final Attribute COLLECTION_NAME_ATTR = new Attribute(COLLECTION_NAME, FieldType.STRING);
    public static final Attribute COLLECTION_DIRECTORY_ATTR = new Attribute(COLLECTION_DIRECTORY, FieldType.STRING);
    public static final Attribute COLLECTION_LUCENE_ANALYZER_ATTR = new Attribute(COLLECTION_LUCENE_ANALYZER,
            FieldType.STRING);

    public static final Schema COLLECTION_CATALOG_SCHEMA = new Schema(COLLECTION_NAME_ATTR, COLLECTION_DIRECTORY_ATTR,
            COLLECTION_LUCENE_ANALYZER_ATTR);

    // Schema for "schema catalog" Collection
    public static final String ATTR_NAME = "attributeName";
    public static final String ATTR_TYPE = "attributeType";
    public static final String ATTR_POSITION = "attributePosition";

    public static final Attribute ATTR_NAME_ATTR = new Attribute(ATTR_NAME, FieldType.STRING);
    public static final Attribute ATTR_TYPE_ATTR = new Attribute(ATTR_TYPE, FieldType.STRING);
    public static final Attribute ATTR_POSITION_ATTR = new Attribute(ATTR_POSITION, FieldType.INTEGER);

    public static final Schema SCHEMA_CATALOG_SCHEMA = new Schema(COLLECTION_NAME_ATTR, ATTR_NAME_ATTR, ATTR_TYPE_ATTR,
            ATTR_POSITION_ATTR);

    public static List<ITuple> getInitialCollectionCatalogTuples() {
        String collectionCatalogDirectoryAbsolute = new File(COLLECTION_CATALOG_DIRECTORY).getAbsolutePath();
        IField[] collectionFields = { new StringField(COLLECTION_CATALOG), new StringField(collectionCatalogDirectoryAbsolute),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER) };
        String schemaCatalogDirectoryAbsolute = new File(SCHEMA_CATALOG_DIRECTORY).getAbsolutePath();
        IField[] schemaFields = { new StringField(SCHEMA_CATALOG), new StringField(schemaCatalogDirectoryAbsolute),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER) };

        List<IField[]> fieldsList = Arrays.asList(collectionFields, schemaFields);
        return fieldsList.stream().map(fields -> new DataTuple(CatalogConstants.COLLECTION_CATALOG_SCHEMA, fields))
                .collect(Collectors.toList());
    }

    public static List<ITuple> getInitialSchemaCatalogTuples() {
        IField[] collectionNameFields = { new StringField(COLLECTION_CATALOG),
                new StringField(CatalogConstants.COLLECTION_NAME), new StringField("string"), new IntegerField(0) };
        IField[] collectionDirectoryFields = { new StringField(COLLECTION_CATALOG),
                new StringField(CatalogConstants.COLLECTION_DIRECTORY), new StringField("string"), new IntegerField(1) };
        IField[] collectionLuceneAnalyzerFields = { new StringField(COLLECTION_CATALOG),
                new StringField(CatalogConstants.COLLECTION_LUCENE_ANALYZER), new StringField("string"),
                new IntegerField(2) };

        IField[] schemaCollectionNameFields = { new StringField(SCHEMA_CATALOG),
                new StringField(CatalogConstants.COLLECTION_NAME), new StringField("string"), new IntegerField(0) };
        IField[] schemaAttrNameFields = { new StringField(SCHEMA_CATALOG), new StringField(CatalogConstants.ATTR_NAME),
                new StringField("string"), new IntegerField(1) };
        IField[] schemaAttrTypeFields = { new StringField(SCHEMA_CATALOG), new StringField(CatalogConstants.ATTR_TYPE),
                new StringField("string"), new IntegerField(2) };
        IField[] schemaAttrPosFields = { new StringField(SCHEMA_CATALOG),
                new StringField(CatalogConstants.ATTR_POSITION), new StringField("integer"), new IntegerField(3) };

        List<IField[]> fieldsList = Arrays.asList(collectionNameFields, collectionDirectoryFields,
                collectionLuceneAnalyzerFields, schemaCollectionNameFields, schemaAttrNameFields, schemaAttrTypeFields,
                schemaAttrPosFields);

        return fieldsList.stream().map(fields -> new DataTuple(CatalogConstants.SCHEMA_CATALOG_SCHEMA, fields))
                .collect(Collectors.toList());
    }

}
