package edu.uci.ics.textdb.common.utils;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.common.exception.StorageException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.json.JSONWriter;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

public class Utils {
    public static IField getField(FieldType fieldType, String fieldValue) throws ParseException {
        IField field = null;
        switch (fieldType) {
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
            field = new DateField(DateTools.stringToDate(fieldValue));
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

    public static IndexableField getLuceneField(FieldType fieldType, String fieldName, Object fieldValue) {
        IndexableField luceneField = null;
        switch (fieldType) {
        // _ID_TYPE is currently same as STRING
        case _ID_TYPE:
        case STRING:
            luceneField = new org.apache.lucene.document.StringField(fieldName, (String) fieldValue, Store.YES);
            break;
        case INTEGER:
            luceneField = new org.apache.lucene.document.IntField(fieldName, (Integer) fieldValue, Store.YES);
            break;
        case DOUBLE:
            double value = (Double) fieldValue;
            luceneField = new org.apache.lucene.document.DoubleField(fieldName, value, Store.YES);
            break;
        case DATE:
            String dateString = DateTools.dateToString((Date) fieldValue, Resolution.MILLISECOND);
            luceneField = new org.apache.lucene.document.StringField(fieldName, dateString, Store.YES);
            break;
        case TEXT:
            // By default we enable positional indexing in Lucene so that we can
            // return
            // information about character offsets and token offsets
            org.apache.lucene.document.FieldType luceneFieldType = new org.apache.lucene.document.FieldType();
            luceneFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            luceneFieldType.setStored(true);
            luceneFieldType.setStoreTermVectors(true);
            luceneFieldType.setStoreTermVectorOffsets(true);
            luceneFieldType.setStoreTermVectorPayloads(true);
            luceneFieldType.setStoreTermVectorPositions(true);
            luceneFieldType.setTokenized(true);

            luceneField = new org.apache.lucene.document.Field(fieldName, (String) fieldValue, luceneFieldType);

            break;
        case LIST:
            // Lucene doesn't have list field
            // WARNING! This case should never be reached.
            break;
        }
        return luceneField;
    }
    
    /**
     * Returns the FieldType of a field object.
     * 
     * @param field
     * @return
     */
    public static FieldType getFieldType(IField field) {
        if (field instanceof DateField) {
            return FieldType.DATE;
        } else if (field instanceof DoubleField) {
            return FieldType.DOUBLE;
        } else if (field instanceof IDField) {
            return FieldType._ID_TYPE;
        } else if (field instanceof IntegerField) {
            return FieldType.INTEGER;
        } else if (field instanceof ListField) {
            return FieldType.LIST;
        } else if (field instanceof StringField) {
            return FieldType.STRING;
        } else if (field instanceof TextField) {
            return FieldType.TEXT;
        } else {
            throw new RuntimeException("no existing type mapping of this field object");
        }
    }

    /**
     * @about Creating a new span tuple from span schema, field list
     */
    public static ITuple getSpanTuple(List<IField> fieldList, List<Span> spanList, Schema spanSchema) {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(spanSchema, fieldsDuplicate);
    }
    
    /**
     * Converts a list of attributes to a list of attribute names
     * 
     * @param attributeList, a list of attributes
     * @return a list of attribute names
     */
    public static List<String> getAttributeNames(List<Attribute> attributeList) {
        return attributeList.stream()
                .map(attr -> attr.getFieldName())
                .collect(Collectors.toList());
    }
    
    /**
     * Converts a list of attributes to a list of attribute names
     * 
     * @param attributeList, a list of attributes
     * @return a list of attribute names
     */
    public static List<String> getAttributeNames(Attribute... attributeList) {
        return Arrays.asList(attributeList).stream()
                .map(attr -> attr.getFieldName())
                .collect(Collectors.toList());
    }
    
    /**
     * Creates a new schema object, with "_ID" attribute added to the front.
     * If the schema already contains "_ID" attribute, returns the original schema.
     * 
     * @param schema
     * @return
     */
    public static Schema getSchemaWithID(Schema schema) {
        if (schema.containsField(SchemaConstants._ID)) {
            return schema;
        }
        
        List<Attribute> attributeList = new ArrayList<>();
        attributeList.add(SchemaConstants._ID_ATTRIBUTE);
        attributeList.addAll(schema.getAttributes());
        return new Schema(attributeList.stream().toArray(Attribute[]::new));      
    }

    /**
     *
     * @param schema
     * @about Creating a new schema object, and adding SPAN_LIST_ATTRIBUTE to
     *        the schema. SPAN_LIST_ATTRIBUTE is of type List
     */
    public static Schema createSpanSchema(Schema schema) {
        return addAttributeToSchema(schema, SchemaConstants.SPAN_LIST_ATTRIBUTE);
    }

    /**
     * Add an attribute to an existing schema (if the attribute doesn't exist).
     * 
     * @param schema
     * @param attribute
     * @return new schema
     */
    public static Schema addAttributeToSchema(Schema schema, Attribute attribute) {
        if (schema.containsField(attribute.getFieldName())) {
            return schema;
        }
        List<Attribute> attributes = new ArrayList<>(schema.getAttributes());
        attributes.add(attribute);
        Schema newSchema = new Schema(attributes.toArray(new Attribute[attributes.size()]));
        return newSchema;
    }
    
    /**
     * Removes one or more attributes from the schema and returns the new schema.
     * 
     * @param schema
     * @param attributeName
     * @return
     */
    public static Schema removeAttributeFromSchema(Schema schema, String... attributeName) {
        return new Schema(schema.getAttributes().stream()
                .filter(attr -> (! Arrays.asList(attributeName).contains(attr.getFieldName())))
                .toArray(Attribute[]::new));
    }

    /**
     * Tokenizes the query string using the given analyser
     * 
     * @param luceneAnalyzer
     * @param query
     * @return ArrayList<String> list of results
     */
    public static ArrayList<String> tokenizeQuery(Analyzer luceneAnalyzer, String query) {
        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                result.add(term.toString());
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public static ArrayList<String> tokenizeQueryWithStopwords(String query) {
        ArrayList<String> result = new ArrayList<String>();
        CharArraySet emptyStopwords = new CharArraySet(1, true);
        Analyzer luceneAnalyzer = new StandardAnalyzer(emptyStopwords);
        TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try {
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String token = term.toString();
                int tokenIndex = query.toLowerCase().indexOf(token);
                // Since tokens are converted to lower case,
                // get the exact token from the query string.
                String actualQueryToken = query.substring(tokenIndex, tokenIndex + token.length());
                result.add(actualQueryToken);
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        luceneAnalyzer.close();

        return result;
    }
    
    
    public static JSONArray getTupleListJSON(List<ITuple> tupleList) {
        JSONArray jsonArray = new JSONArray();
        
        for (ITuple tuple : tupleList) {
            jsonArray.put(getTupleJSON(tuple));
        }
        
        return jsonArray;
    }
    
    public static JSONObject getTupleJSON(ITuple tuple) {
        JSONObject jsonObject = new JSONObject();
        
        for (String attrName : tuple.getSchema().getAttributeNames()) {
            if (attrName.equalsIgnoreCase(SchemaConstants.SPAN_LIST)) {
                List<Span> spanList = ((ListField<Span>) tuple.getField(SchemaConstants.SPAN_LIST)).getValue();
                jsonObject.put(attrName, getSpanListJSON(spanList));
            } else {
                jsonObject.put(attrName, tuple.getField(attrName).getValue().toString());
            }
        }
        
        return jsonObject;
    }
    
    public static JSONArray getSpanListJSON(List<Span> spanList) {
        JSONArray jsonArray = new JSONArray();
        
        for (Span span : spanList) {
            jsonArray.put(getSpanJSON(span));
        }
        
        return jsonArray;
    }
    
    public static JSONObject getSpanJSON(Span span) {
        JSONObject jsonObject = new JSONObject();
        
        jsonObject.put("key", span.getKey());
        jsonObject.put("value", span.getValue());
        jsonObject.put("field", span.getFieldName());
        jsonObject.put("start", span.getStart());
        jsonObject.put("end", span.getEnd());
        jsonObject.put("token offset", span.getTokenOffset());

        return jsonObject;
    }

    public static String getTupleListString(List<ITuple> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (ITuple tuple : tupleList) {
            sb.append(getTupleString(tuple));
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Transform a tuple into string
     * 
     * @param tuple
     * @return string representation of the tuple
     */
    public static String getTupleString(ITuple tuple) {
        StringBuilder sb = new StringBuilder();

        Schema schema = tuple.getSchema();
        for (Attribute attribute : schema.getAttributes()) {
            if (attribute.getFieldName().equals(SchemaConstants.SPAN_LIST)) {
                List<Span> spanList = ((ListField<Span>) tuple.getField(SchemaConstants.SPAN_LIST)).getValue();
                sb.append(getSpanListString(spanList));
                sb.append("\n");
            } else {
                sb.append(attribute.getFieldName());
                sb.append("(");
                sb.append(attribute.getFieldType().toString());
                sb.append(")");
                sb.append(": ");
                sb.append(tuple.getField(attribute.getFieldName()).getValue().toString());
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Transform a list of spans into string
     * 
     * @param tuple
     * @return string representation of a list of spans
     */
    public static String getSpanListString(List<Span> spanList) {
        StringBuilder sb = new StringBuilder();

        sb.append("span list:\n");
        for (Span span : spanList) {
            sb.append(getSpanString(span));
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Transform a span into string
     * 
     * @param tuple
     * @return string representation of a span
     */
    public static String getSpanString(Span span) {
        StringBuilder sb = new StringBuilder();

        sb.append("field: " + span.getFieldName() + "\n");
        sb.append("start: " + span.getStart() + "\n");
        sb.append("end:   " + span.getEnd() + "\n");
        sb.append("key:   " + span.getKey() + "\n");
        sb.append("value: " + span.getValue() + "\n");
        sb.append("token offset: " + span.getTokenOffset() + "\n");

        return sb.toString();
    }

    /**
     * Remove one or more fields from each tuple in tupleList.
     * 
     * @param tupleList
     * @param removeFields
     * @return
     */
    public static List<ITuple> removeFields(List<ITuple> tupleList, String... removeFields) {
        List<ITuple> newTuples = tupleList.stream().map(tuple -> removeFields(tuple, removeFields))
                .collect(Collectors.toList());
        return newTuples;
    }
    
    /**
     * Remove one or more fields from a tuple.
     * 
     * @param tuple
     * @param removeFields
     * @return
     */
    public static ITuple removeFields(ITuple tuple, String... removeFields) {
        List<String> removeFieldList = Arrays.asList(removeFields);
        List<Integer> removedFeidsIndex = removeFieldList.stream()
                .map(fieldName -> tuple.getSchema().getIndex(fieldName)).collect(Collectors.toList());
        
        Attribute[] newAttrs = tuple.getSchema().getAttributes().stream()
                .filter(attr -> (! removeFieldList.contains(attr.getFieldName()))).toArray(Attribute[]::new);
        Schema newSchema = new Schema(newAttrs);
        
        IField[] newFields = IntStream.range(0, tuple.getSchema().getAttributes().size())
            .filter(index -> (! removedFeidsIndex.contains(index)))
            .mapToObj(index -> tuple.getField(index)).toArray(IField[]::new);
        
        return new DataTuple(newSchema, newFields);
    }

    public static List<Span> generatePayloadFromTuple(ITuple tuple, Analyzer luceneAnalyzer) {
        List<Span> tuplePayload = tuple.getSchema().getAttributes().stream()
                .filter(attr -> (attr.getFieldType() == FieldType.TEXT)) // generate payload only for TEXT field
                .map(attr -> attr.getFieldName())
                .map(fieldName -> generatePayload(fieldName, tuple.getField(fieldName).getValue().toString(),
                        luceneAnalyzer))
                .flatMap(payload -> payload.stream()) // flatten a list of lists to a list
                .collect(Collectors.toList());

        return tuplePayload;
    }

    public static List<Span> generatePayload(String fieldName, String fieldValue, Analyzer luceneAnalyzer) {
        List<Span> payload = new ArrayList<>();
        
        try {
            TokenStream tokenStream = luceneAnalyzer.tokenStream(null, new StringReader(fieldValue));
            OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute positionIncrementAttribute = 
                    tokenStream.addAttribute(PositionIncrementAttribute.class);
            
            int tokenPositionCounter = -1;
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                tokenPositionCounter += positionIncrementAttribute.getPositionIncrement();
                
                int tokenPosition = tokenPositionCounter;
                int charStart = offsetAttribute.startOffset();
                int charEnd = offsetAttribute.endOffset();
                String analyzedTermStr = charTermAttribute.toString();
                String originalTermStr = fieldValue.substring(charStart, charEnd);

                payload.add(new Span(fieldName, charStart, charEnd, analyzedTermStr, originalTermStr, tokenPosition));
            }
            tokenStream.close();
        } catch (IOException e) {
            payload.clear(); // return empty payload
        }

        return payload;
    }

    public static void deleteDirectory(String indexDir) throws StorageException {
        Path directory = Paths.get(indexDir);
        if (!Files.exists(directory)) {
            return;
        }

        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new StorageException("failed to delete a given directory", e);
        }
    }
}