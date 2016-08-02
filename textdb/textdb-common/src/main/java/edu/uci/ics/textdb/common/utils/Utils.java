package edu.uci.ics.textdb.common.utils;

import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.DateField;
import edu.uci.ics.textdb.common.field.DoubleField;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.field.StringField;
import edu.uci.ics.textdb.common.field.TextField;

public class Utils {
    public static IField getField(FieldType fieldType, String fieldValue) throws ParseException{
        IField field = null;
        switch (fieldType) {
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

    public static IndexableField getLuceneField(FieldType fieldType,
             String fieldName, Object fieldValue) {
        IndexableField luceneField = null;
        switch(fieldType){
            case STRING:
                luceneField = new org.apache.lucene.document.StringField(
                        fieldName, (String) fieldValue, Store.YES);
                break;
            case INTEGER:
                luceneField = new org.apache.lucene.document.IntField(
                        fieldName, (Integer) fieldValue, Store.YES);
                break;
            case DOUBLE:
                double value = (Double) fieldValue;
                luceneField = new org.apache.lucene.document.DoubleField(
                        fieldName, value, Store.YES);
                break;
            case DATE:
                String dateString = DateTools.dateToString((Date) fieldValue, Resolution.MILLISECOND);
                luceneField = new org.apache.lucene.document.StringField(fieldName, dateString, Store.YES);
                break;
            case TEXT:
                //By default we enable positional indexing in Lucene so that we can return
                // information about character offsets and token offsets
                org.apache.lucene.document.FieldType luceneFieldType = new org.apache.lucene.document.FieldType();
                luceneFieldType.setIndexOptions( IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS );
                luceneFieldType.setStored(true);
                luceneFieldType.setStoreTermVectors( true );
                luceneFieldType.setStoreTermVectorOffsets( true );
                luceneFieldType.setStoreTermVectorPayloads( true );
                luceneFieldType.setStoreTermVectorPositions( true );
                luceneFieldType.setTokenized( true );

                luceneField = new org.apache.lucene.document.Field(
                        fieldName,(String) fieldValue,luceneFieldType);

                break;
            case LIST:
            	// Lucene doesn't have list field
            	// WARNING! This case should never be reached.
            	break;
        }
        return luceneField;
    }
    /**
     * @about Creating a new span tuple from span schema, field list 
     */
    public static ITuple getSpanTuple( List<IField> fieldList, List<Span> spanList, Schema spanSchema) {
        IField spanListField = new ListField<Span>(new ArrayList<>(spanList));
        List<IField> fieldListDuplicate = new ArrayList<>(fieldList);
        fieldListDuplicate.add(spanListField);

        IField[] fieldsDuplicate = fieldListDuplicate.toArray(new IField[fieldListDuplicate.size()]);
        return new DataTuple(spanSchema, fieldsDuplicate);
    }

    /**
     *
     * @param schema
     * @about Creating a new schema object, and adding SPAN_LIST_ATTRIBUTE to
     *        the schema. SPAN_LIST_ATTRIBUTE is of type List
     */
    public static Schema createSpanSchema(Schema schema) {
        List<Attribute> dataTupleAttributes = schema.getAttributes();
        //spanAttributes contains all attributes of dataTupleAttributes and an additional SPAN_LIST_ATTRIBUTE
        Attribute[] spanAttributes = new Attribute[dataTupleAttributes.size() + 1];
        for (int count = 0; count < dataTupleAttributes.size(); count++) {
            spanAttributes[count] = dataTupleAttributes.get(count);
        }
        spanAttributes[spanAttributes.length - 1] = SchemaConstants.SPAN_LIST_ATTRIBUTE;
        Schema spanSchema = new Schema(spanAttributes);
        return spanSchema;
    }

    /**
     * Tokenizes the query string using the given analyser
     * @param luceneAnalyzer
     * @param query
     * @return ArrayList<String> list of results
     */
    public static ArrayList<String> tokenizeQuery(Analyzer luceneAnalyzer, String query) {
        ArrayList<String> result = new ArrayList<String>();
        TokenStream tokenStream  = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try{
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String token = term.toString();
                int tokenIndex = query.toLowerCase().indexOf(token);
                // Since tokens are converted to lower case,
                // get the exact token from the query string.
                String actualQueryToken = query.substring(tokenIndex, tokenIndex+token.length());
                result.add(actualQueryToken);
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
        TokenStream tokenStream  = luceneAnalyzer.tokenStream(null, new StringReader(query));
        CharTermAttribute term = tokenStream.addAttribute(CharTermAttribute.class);

        try{
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                String token = term.toString();
                int tokenIndex = query.toLowerCase().indexOf(token);
                // Since tokens are converted to lower case,
                // get the exact token from the query string.
                String actualQueryToken = query.substring(tokenIndex, tokenIndex+token.length());
                result.add(actualQueryToken);
            }
            tokenStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        luceneAnalyzer.close();

        return result;
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
    		}
    		else {
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
     * @param tuple
     * @return string representation of a span
     */
    public static String getSpanString(Span span) {
    	StringBuilder sb = new StringBuilder();
    	
    	sb.append("field: "+span.getFieldName()+"\n");
    	sb.append("start: "+span.getStart()+"\n");
    	sb.append("end:   "+span.getEnd()+"\n");
    	sb.append("key:   "+span.getKey()+"\n");
    	sb.append("value: "+span.getValue()+"\n");
    	sb.append("token offset: "+span.getTokenOffset()+"\n");
    	
    	return sb.toString();
    }
    
}