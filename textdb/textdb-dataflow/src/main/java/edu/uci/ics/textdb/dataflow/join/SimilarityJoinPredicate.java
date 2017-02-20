package edu.uci.ics.textdb.dataflow.join;

import java.util.*;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IDField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.IJoinPredicate;
import info.debatty.java.stringsimilarity.NormalizedLevenshtein;

/**
 *
 * Similarity Join Predicate is one type of join predicate where
 *   two tuples are joined if the values in their spanlists are
 *   similar within a similarity threshold.
 *
 * The output schema of Similarity Join will be the combination of inner and outer schema.
 *   (except the _id, spanList and payload field)
 * TODO: this solution for output schema is only temporary
 *
 * Currently the similarity is measured by normalized Levenshtein distance,
 *   which is the Levenshtein distance divided by the length of the longest string
 *
 * Example of a same-table, different-tuple join, similarity threshold > 0.8
 *
 * table_schema,   inner_tuple,             outer_tuple
 *   _id:          random_id                random_id
 *   content:      "textdb"                 "testdb"
 *   spanList:     (0, 6, "textdb", content)  (0, 6, "testdb", content)
 *   payload:      payload_inner            payload_outer
 *
 * Similarity of "textdb" and "testdb" is 1 - (1/6) = 0.833
 *
 * result_schema,      result_tuple
 *   _id:              new_random_id
 *   inner_content:    "textdb"
 *   outer_content:    "testdb"
 *   spanList:         [(0, 6, "textdb", inner_content)  (0, 6, "testdb", outer_content)]
 *   payload:          [payload_inner, payload_outer]
 *
 * @author Zuozhi Wang
 *
 */
public class SimilarityJoinPredicate implements IJoinPredicate {
    
    public static final String INNER_PREFIX = "inner_";
    public static final String OUTER_PREFIX = "outer_";
    
    Double similarityThreshold;

    String innerJoinAttrName;
    String outerJoinAttrName;
    
    private SimilarityFunc similarityFunc;
    
    @FunctionalInterface
    public static interface SimilarityFunc {
        Double calculateSimilarity(String str1, String str2);
    }


    public SimilarityJoinPredicate(String joinAttributeName, Double similarityThreshold) {
        this(joinAttributeName, joinAttributeName, similarityThreshold);
    }

    public SimilarityJoinPredicate(String outerJoinAttrName, String innerJoinAttrName, Double similarityThreshold) {
        if (similarityThreshold > 1) {
            similarityThreshold = 1.0;
        } else if (similarityThreshold < 0) {
            similarityThreshold = 0.0;
        }
        this.similarityThreshold = similarityThreshold;
        this.outerJoinAttrName = outerJoinAttrName;
        this.innerJoinAttrName = innerJoinAttrName;
        
        // initialize default similarity function to NormalizedLevenshtein
        // which is Levenshtein distance / length of longest string
        this.similarityFunc = ((str1, str2) -> (1.0 - new NormalizedLevenshtein().distance(str1, str2)));
    }

    
    @Override
    public Schema generateOutputSchema(Schema outerOperatorSchema, Schema innerOperatorSchema) throws DataFlowException {
        List<Attribute> outputAttributeList = new ArrayList<>();
        
        // add _ID field first
        outputAttributeList.add(SchemaConstants._ID_ATTRIBUTE);
        
        for (Attribute attr : innerOperatorSchema.getAttributes()) {
            String attrName = attr.getFieldName();
            FieldType attrType = attr.getFieldType();
            // ignore _id, spanList, and payload
            if (attrName.equals(SchemaConstants._ID) || attrName.equals(SchemaConstants.SPAN_LIST) 
                    || attrName.equals(SchemaConstants.PAYLOAD)) {
                continue;
            }
            outputAttributeList.add(new Attribute(INNER_PREFIX + attrName, attrType));
        }
        for (Attribute attr : outerOperatorSchema.getAttributes()) {
            String attrName = attr.getFieldName();
            FieldType attrType = attr.getFieldType();
            // ignore _id, spanList, and payload
            if (attrName.equals(SchemaConstants._ID) || attrName.equals(SchemaConstants.SPAN_LIST) 
                    || attrName.equals(SchemaConstants.PAYLOAD)) {
                continue;
            }
            outputAttributeList.add(new Attribute(OUTER_PREFIX + attrName, attrType));
        }
        
        // add spanList field
        outputAttributeList.add(SchemaConstants.SPAN_LIST_ATTRIBUTE);

        // add payload field if one of them contains payload
        if (innerOperatorSchema.containsField(SchemaConstants.PAYLOAD) || 
                outerOperatorSchema.containsField(SchemaConstants.PAYLOAD)) {
            outputAttributeList.add(SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        
        return new Schema(outputAttributeList.stream().toArray(Attribute[]::new));
    }

    @Override
    public ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema) throws DataFlowException {        
        if (similarityThreshold == 0) {
            return null;
        }
        
        // get the span list only with the joinAttributeName
        List<Span> innerRelevantSpanList = ((ListField<Span>) innerTuple.getField(SchemaConstants.SPAN_LIST))
                .getValue().stream().filter(span -> span.getFieldName().equals(innerJoinAttrName)).collect(Collectors.toList());
        List<Span> outerRelevantSpanList = ((ListField<Span>) outerTuple.getField(SchemaConstants.SPAN_LIST))
                .getValue().stream().filter(span -> span.getFieldName().equals(outerJoinAttrName)).collect(Collectors.toList());
        
        // get a set of span's values (since multiple spans may have the same value)
        Set<String> innerSpanValueSet = innerRelevantSpanList.stream()
                .map(span -> span.getValue()).collect(Collectors.toSet());
        Set<String> outerSpanValueSet = outerRelevantSpanList.stream()
                .map(span -> span.getValue()).collect(Collectors.toSet());

        // compute the result value set using the similarity function
        Set<String> resultValueSet = new HashSet<>();
        for (String innerString : innerSpanValueSet) {
            for (String outerString : outerSpanValueSet) {
                if (this.similarityFunc.calculateSimilarity(innerString, outerString) >= this.similarityThreshold ) {
                    resultValueSet.add(innerString);
                    resultValueSet.add(outerString);
                }
            }
        }
        
        // return null if none of them are similar
        if (resultValueSet.isEmpty()) {
            return null;
        }
        
        // generate the result spans
        List<Span> resultSpans = new ArrayList<>();
        for (Span span : innerRelevantSpanList) {
            if (resultValueSet.contains(span.getValue())) {
                resultSpans.add(addFieldPrefix(span, INNER_PREFIX));
            }
        }
        for (Span span : outerRelevantSpanList) {
            if (resultValueSet.contains(span.getValue())) {
                resultSpans.add(addFieldPrefix(span, OUTER_PREFIX));
            }
        }
                
        return mergeTuples(outerTuple, innerTuple, outputSchema, resultSpans);
    }
    
    
    private ITuple mergeTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema, List<Span> mergeSpanList) {
        List<IField> resultFields = new ArrayList<>();
        for (String attrName : outputSchema.getAttributeNames()) {
            // generate a new _ID field for this tuple
            if (attrName.equals(SchemaConstants._ID)) {
                IDField newID = new IDField(UUID.randomUUID().toString());
                resultFields.add(newID);
            // use the generated spanList
            } else if (attrName.equals(SchemaConstants.SPAN_LIST)) {
                resultFields.add(new ListField<Span>(mergeSpanList));
            // put the payload of two tuples together
            } else if (attrName.equals(SchemaConstants.PAYLOAD)) {
                List<Span> innerPayload = ((ListField<Span>) innerTuple.getField(SchemaConstants.PAYLOAD)).getValue();
                List<Span> outerPayload = ((ListField<Span>) outerTuple.getField(SchemaConstants.PAYLOAD)).getValue();
                
                List<Span> resultPayload = new ArrayList<>();
                resultPayload.addAll(innerPayload.stream().map(span -> addFieldPrefix(span, INNER_PREFIX)).collect(Collectors.toList()));
                resultPayload.addAll(outerPayload.stream().map(span -> addFieldPrefix(span, "outer_")).collect(Collectors.toList()));
            // add other fields from inner/outer tuples
            } else {
                if (attrName.startsWith(INNER_PREFIX)) {
                    resultFields.add(innerTuple.getField(attrName.substring(INNER_PREFIX.length())));
                } else if (attrName.startsWith(OUTER_PREFIX)) {
                    resultFields.add(outerTuple.getField(attrName.substring(OUTER_PREFIX.length())));
                }
            }
        }
        return new DataTuple(outputSchema, resultFields.stream().toArray(IField[]::new));
    }
    
    private Span addFieldPrefix(Span span, String prefix) {
        return new Span(prefix+span.getFieldName(), 
                span.getStart(), span.getEnd(), span.getKey(), span.getValue(), span.getTokenOffset());
    }

    @Override
    public String getInnerAttributeName() {
        return this.innerJoinAttrName;
    }

    @Override
    public String getOuterAttributeName() {
        return this.outerJoinAttrName;
    }
    
    public Double getThreshold() {
        return this.similarityThreshold;
    }
    
    public void setSimilarityFunction(SimilarityFunc similarityFunc) {
        this.similarityFunc = similarityFunc;
    }

}
