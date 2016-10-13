package edu.uci.ics.textdb.storage.catalog;

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

    public static final String DOCUMENT_CATALOG = "documentCatalog";
    public static final String SCHEMA_CATALOG = "schemaCatalog";

    public static final String DOCUMENT_CATALOG_DIRECTORY = "../catalog/document";
    public static final String SCHEMA_CATALOG_DIRECTORY = "../catalog/schema";

    // Schema for the "document catalog" Document
    public static final String DOCUMENT_NAME = "documentName";
    public static final String DOCUMENT_DIRECTORY = "documentDirectory";
    public static final String DOCUMENT_LUCENE_ANALYZER = "luceneAnalyzer";

    public static final Attribute DOCUMENT_NAME_ATTR = new Attribute(DOCUMENT_NAME, FieldType.STRING);
    public static final Attribute DOCUMENT_DIRECTORY_ATTR = new Attribute(DOCUMENT_DIRECTORY, FieldType.STRING);
    public static final Attribute DOCUMENT_LUCENE_ANALYZER_ATTR = new Attribute(DOCUMENT_LUCENE_ANALYZER,
            FieldType.STRING);

    public static final Schema CATALOG_DOCUMENT_SCHEMA = new Schema(DOCUMENT_NAME_ATTR, DOCUMENT_DIRECTORY_ATTR,
            DOCUMENT_LUCENE_ANALYZER_ATTR);

    // Schema for "schema catalog" Document
    public static final String ATTR_NAME = "attributeName";
    public static final String ATTR_TYPE = "attributeType";
    public static final String ATTR_POSITION = "attributePosition";

    public static final Attribute ATTR_NAME_ATTR = new Attribute(ATTR_NAME, FieldType.STRING);
    public static final Attribute ATTR_TYPE_ATTR = new Attribute(ATTR_TYPE, FieldType.STRING);
    public static final Attribute ATTR_POSITION_ATTR = new Attribute(ATTR_POSITION, FieldType.INTEGER);

    public static final Schema CATALOG_SCHEMA_SCHEMA = new Schema(DOCUMENT_NAME_ATTR, ATTR_NAME_ATTR, ATTR_TYPE_ATTR,
            ATTR_POSITION_ATTR);

    public static List<ITuple> getInitialDocumentCatalogTuples() {
        IField[] documentFields = { new StringField(DOCUMENT_CATALOG), new StringField(DOCUMENT_CATALOG_DIRECTORY),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER) };
        IField[] schemaFields = { new StringField(SCHEMA_CATALOG), new StringField(SCHEMA_CATALOG_DIRECTORY),
                new StringField(DataConstants.STANDARD_LUCENE_ANALYZER) };

        List<IField[]> fieldsList = Arrays.asList(documentFields, schemaFields);
        return fieldsList.stream().map(fields -> new DataTuple(CatalogConstants.CATALOG_DOCUMENT_SCHEMA, fields))
                .collect(Collectors.toList());
    }

    public static List<ITuple> getInitialSchemaCatalogTuples() {
        IField[] documentNameFields = { new StringField(DOCUMENT_CATALOG),
                new StringField(CatalogConstants.DOCUMENT_NAME), new StringField("string"), new IntegerField(0) };
        IField[] documentDirectoryFields = { new StringField(DOCUMENT_CATALOG),
                new StringField(CatalogConstants.DOCUMENT_DIRECTORY), new StringField("string"), new IntegerField(1) };
        IField[] documentLuceneAnalyzerFields = { new StringField(DOCUMENT_CATALOG),
                new StringField(CatalogConstants.DOCUMENT_LUCENE_ANALYZER), new StringField("string"),
                new IntegerField(2) };

        IField[] schemaDocumentNameFields = { new StringField(SCHEMA_CATALOG),
                new StringField(CatalogConstants.DOCUMENT_NAME), new StringField("string"), new IntegerField(0) };
        IField[] schemaAttrNameFields = { new StringField(SCHEMA_CATALOG), new StringField(CatalogConstants.ATTR_NAME),
                new StringField("string"), new IntegerField(1) };
        IField[] schemaAttrTypeFields = { new StringField(SCHEMA_CATALOG), new StringField(CatalogConstants.ATTR_TYPE),
                new StringField("string"), new IntegerField(2) };
        IField[] schemaAttrPosFields = { new StringField(SCHEMA_CATALOG),
                new StringField(CatalogConstants.ATTR_POSITION), new StringField("integer"), new IntegerField(3) };

        List<IField[]> fieldsList = Arrays.asList(documentNameFields, documentDirectoryFields,
                documentLuceneAnalyzerFields, schemaDocumentNameFields, schemaAttrNameFields, schemaAttrTypeFields,
                schemaAttrPosFields);

        return fieldsList.stream().map(fields -> new DataTuple(CatalogConstants.CATALOG_SCHEMA_SCHEMA, fields))
                .collect(Collectors.toList());
    }

}
