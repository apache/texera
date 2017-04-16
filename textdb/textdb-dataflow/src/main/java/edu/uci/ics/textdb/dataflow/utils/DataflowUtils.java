package edu.uci.ics.textdb.dataflow.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.storage.TableMetadata;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.uci.ics.textdb.api.constants.SchemaConstants;
import edu.uci.ics.textdb.api.field.DateField;
import edu.uci.ics.textdb.api.field.DoubleField;
import edu.uci.ics.textdb.api.field.IDField;
import edu.uci.ics.textdb.api.field.IField;
import edu.uci.ics.textdb.api.field.IntegerField;
import edu.uci.ics.textdb.api.field.ListField;
import edu.uci.ics.textdb.api.field.StringField;
import edu.uci.ics.textdb.api.field.TextField;
import edu.uci.ics.textdb.api.schema.Attribute;
import edu.uci.ics.textdb.api.schema.AttributeType;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.span.Span;
import edu.uci.ics.textdb.api.tuple.*;

public class DataflowUtils {
    
    /**
     * Returns the AttributeType of a field object.
     * 
     * @param field
     * @return
     */
    public static AttributeType getAttributeType(IField field) {
        if (field instanceof DateField) {
            return AttributeType.DATE;
        } else if (field instanceof DoubleField) {
            return AttributeType.DOUBLE;
        } else if (field instanceof IDField) {
            return AttributeType._ID_TYPE;
        } else if (field instanceof IntegerField) {
            return AttributeType.INTEGER;
        } else if (field instanceof ListField) {
            return AttributeType.LIST;
        } else if (field instanceof StringField) {
            return AttributeType.STRING;
        } else if (field instanceof TextField) {
            return AttributeType.TEXT;
        } else {
            throw new RuntimeException("no existing type mapping of this field object");
        }
    }

    /**
     * @about Creating a new span tuple from span schema, field list
     */
    public static Tuple getSpanTuple(List<IField> fieldList, List<Span> spanList, Schema spanSchema) {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new Tuple(spanSchema, fieldsDuplicate);
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
    
    
    public static JSONArray getTupleListJSON(List<Tuple> tupleList) {
        JSONArray jsonArray = new JSONArray();
        
        for (Tuple tuple : tupleList) {
            jsonArray.put(getTupleJSON(tuple));
        }
        
        return jsonArray;
    }
    
    public static JSONObject getTupleJSON(Tuple tuple) {
        JSONObject jsonObject = new JSONObject();
        
        for (String attrName : tuple.getSchema().getAttributeNames()) {
            if (attrName.equalsIgnoreCase(SchemaConstants.SPAN_LIST)) {
                ListField<Span> spanListField = tuple.getField(SchemaConstants.SPAN_LIST);
                List<Span> spanList = spanListField.getValue();
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
        jsonObject.put("field", span.getAttributeName());
        jsonObject.put("start", span.getStart());
        jsonObject.put("end", span.getEnd());
        jsonObject.put("token offset", span.getTokenOffset());

        return jsonObject;
    }

    public static String getTupleListString(List<Tuple> tupleList) {
        StringBuilder sb = new StringBuilder();
        for (Tuple tuple : tupleList) {
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
    public static String getTupleString(Tuple tuple) {
        StringBuilder sb = new StringBuilder();

        Schema schema = tuple.getSchema();
        for (Attribute attribute : schema.getAttributes()) {
            if (attribute.getAttributeName().equals(SchemaConstants.SPAN_LIST)) {
                ListField<Span> spanListField = tuple.getField(SchemaConstants.SPAN_LIST);
                List<Span> spanList = spanListField.getValue();
                sb.append(getSpanListString(spanList));
                sb.append("\n");
            } else {
                sb.append(attribute.getAttributeName());
                sb.append("(");
                sb.append(attribute.getAttributeType().toString());
                sb.append(")");
                sb.append(": ");
                sb.append(tuple.getField(attribute.getAttributeName()).getValue().toString());
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

        sb.append("field: " + span.getAttributeName() + "\n");
        sb.append("start: " + span.getStart() + "\n");
        sb.append("end:   " + span.getEnd() + "\n");
        sb.append("key:   " + span.getKey() + "\n");
        sb.append("value: " + span.getValue() + "\n");
        sb.append("token offset: " + span.getTokenOffset() + "\n");

        return sb.toString();
    }

    public static List<Span> generatePayloadFromTuple(Tuple tuple, Analyzer luceneAnalyzer) {
        List<Span> tuplePayload = tuple.getSchema().getAttributes().stream()
                .filter(attr -> (attr.getAttributeType() == AttributeType.TEXT)) // generate payload only for TEXT field
                .map(attr -> attr.getAttributeName())
                .map(attributeName -> generatePayload(attributeName, tuple.getField(attributeName).getValue().toString(),
                        luceneAnalyzer))
                .flatMap(payload -> payload.stream()) // flatten a list of lists to a list
                .collect(Collectors.toList());

        return tuplePayload;
    }

    public static List<Span> generatePayload(String attributeName, String fieldValue, Analyzer luceneAnalyzer) {
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

                payload.add(new Span(attributeName, charStart, charEnd, analyzedTermStr, originalTermStr, tokenPosition));
            }
            tokenStream.close();
        } catch (IOException e) {
            payload.clear(); // return empty payload
        }

        return payload;
    }

    public static JSONArray getMetadataJSON(List<TableMetadata> list) {
        JSONArray jsonArray = new JSONArray();

        for (TableMetadata t : list) {
            JSONObject metadataObject = new JSONObject();
            metadataObject.put("tableName", t.getTableName());

            JSONArray attributeArray = new JSONArray();
            for (Attribute attribute : t.getSchema().getAttributes()) {
                JSONObject attributeObject = new JSONObject();
                attributeObject.put("attributeName", attribute.getAttributeName());
                attributeObject.put("attributeType", attribute.getAttributeType());
                attributeArray.put(attributeObject);
            }

            metadataObject.put("attributes", attributeArray);
            jsonArray.put(metadataObject);
        }

        return jsonArray;
    }
}