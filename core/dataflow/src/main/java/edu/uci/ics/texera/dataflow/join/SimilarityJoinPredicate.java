package edu.uci.ics.texera.dataflow.join;

import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.*;
import edu.uci.ics.texera.dataflow.common.OperatorGroupConstants;
import edu.uci.ics.texera.dataflow.common.PredicateBase;
import edu.uci.ics.texera.dataflow.common.PropertyNameConstants;
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
 *   content:      "texera"                 "testdb"
 *   spanList:     (0, 6, "texera", content)  (0, 6, "testdb", content)
 *   payload:      payload_inner            payload_outer
 *
 * Similarity of "texera" and "testdb" is 1 - (1/6) = 0.833
 *
 * result_schema,      result_tuple
 *   _id:              new_random_id
 *   inner_content:    "texera"
 *   outer_content:    "testdb"
 *   spanList:         [(0, 6, "texera", inner_content)  (0, 6, "testdb", outer_content)]
 *   payload:          [payload_inner, payload_outer]
 *
 * @author Zuozhi Wang
 *
 */
public class SimilarityJoinPredicate extends PredicateBase implements IJoinPredicate {
    
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
    
    @JsonCreator
    public SimilarityJoinPredicate(
            @JsonProperty(value = PropertyNameConstants.INNER_ATTRIBUTE_NAME, required = true)
            String innerJoinAttrName, 
            @JsonProperty(value = PropertyNameConstants.OUTER_ATTRIBUTE_NAME, required = true)
            String outerJoinAttrName, 
            @JsonProperty(value = PropertyNameConstants.JOIN_SIMILARITY_THRESHOLD, required = true)
            Double similarityThreshold) {
        if (similarityThreshold > 1) {
            similarityThreshold = 1.0;
        } else if (similarityThreshold < 0) {
            similarityThreshold = 0.0;
        }
        this.similarityThreshold = similarityThreshold;
        this.innerJoinAttrName = innerJoinAttrName;
        this.outerJoinAttrName = outerJoinAttrName;
        
        // initialize default similarity function to NormalizedLevenshtein
        // which is Levenshtein distance / length of longest string
        this.similarityFunc = ((str1, str2) -> (1.0 - new NormalizedLevenshtein().distance(str1, str2)));
    }
    
    @JsonProperty(value = PropertyNameConstants.INNER_ATTRIBUTE_NAME)
    @Override
    public String getInnerAttributeName() {
        return this.innerJoinAttrName;
    }

    @JsonProperty(value = PropertyNameConstants.OUTER_ATTRIBUTE_NAME)
    @Override
    public String getOuterAttributeName() {
        return this.outerJoinAttrName;
    }
    
    @JsonProperty(value = PropertyNameConstants.JOIN_SIMILARITY_THRESHOLD)
    public Double getThreshold() {
        return this.similarityThreshold;
    }

    
    @Override
    public Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataflowException {
        List<Attribute> outputAttributeList = new ArrayList<>();
        
        // add _ID field first
        outputAttributeList.add(SchemaConstants._ID_ATTRIBUTE);
        
        for (Attribute attr : innerOperatorSchema.getAttributes()) {
            String attrName = attr.getName();
            AttributeType attrType = attr.getType();
            // ignore _id, spanList, and payload
            if (attrName.equals(SchemaConstants._ID) || attrName.equals(SchemaConstants.SPAN_LIST) 
                    || attrName.equals(SchemaConstants.PAYLOAD)) {
                continue;
            }
            outputAttributeList.add(new Attribute(INNER_PREFIX + attrName, attrType));
        }
        for (Attribute attr : outerOperatorSchema.getAttributes()) {
            String attrName = attr.getName();
            AttributeType attrType = attr.getType();
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
        if (innerOperatorSchema.containsAttribute(SchemaConstants.PAYLOAD) || 
                outerOperatorSchema.containsAttribute(SchemaConstants.PAYLOAD)) {
            outputAttributeList.add(SchemaConstants.PAYLOAD_ATTRIBUTE);
        }
        
        return new Schema(outputAttributeList.stream().toArray(Attribute[]::new));
    }

    @Override
    public Tuple joinTuples(Tuple innerTuple, Tuple outerTuple, Schema outputSchema) throws DataflowException {        
        if (similarityThreshold == 0) {
            return null;
        }
        
        // get the span list only with the joinAttributeName
        ListField<Span> innerSpanListField = innerTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> innerRelevantSpanList = innerSpanListField.getValue().stream()
                .filter(span -> span.getAttributeName().equals(innerJoinAttrName)).collect(Collectors.toList());
        
        ListField<Span> outerSpanListField = outerTuple.getField(SchemaConstants.SPAN_LIST);
        List<Span> outerRelevantSpanList = outerSpanListField.getValue().stream()
                .filter(span -> span.getAttributeName().equals(outerJoinAttrName)).collect(Collectors.toList());
        
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
                
        return mergeTuples(innerTuple, outerTuple, outputSchema, resultSpans);
    }
    
    
    private Tuple mergeTuples(Tuple innerTuple, Tuple outerTuple, Schema outputSchema, List<Span> mergeSpanList) {
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
                ListField<Span> innerPayloadField = innerTuple.getField(SchemaConstants.PAYLOAD);
                List<Span> innerPayload = innerPayloadField.getValue();      
                ListField<Span> outerPayloadField = outerTuple.getField(SchemaConstants.PAYLOAD);
                List<Span> outerPayload = outerPayloadField.getValue();
                
                List<Span> resultPayload = new ArrayList<>();
                resultPayload.addAll(innerPayload.stream().map(span -> addFieldPrefix(span, INNER_PREFIX)).collect(Collectors.toList()));
                resultPayload.addAll(outerPayload.stream().map(span -> addFieldPrefix(span, "outer_")).collect(Collectors.toList()));
                
                resultFields.add(new ListField<Span>(resultPayload));
            // add other fields from inner/outer tuples
            } else {
                if (attrName.startsWith(INNER_PREFIX)) {
                    resultFields.add(innerTuple.getField(attrName.substring(INNER_PREFIX.length())));
                } else if (attrName.startsWith(OUTER_PREFIX)) {
                    resultFields.add(outerTuple.getField(attrName.substring(OUTER_PREFIX.length())));
                }
            }
        }
        return new Tuple(outputSchema, resultFields);
    }
    
    private Span addFieldPrefix(Span span, String prefix) {
        return new Span(prefix+span.getAttributeName(),
                span.getStart(), span.getEnd(), span.getKey(), span.getValue(), span.getTokenOffset());
    }
    
    @JsonIgnore
    public void setSimilarityFunction(SimilarityFunc similarityFunc) {
        this.similarityFunc = similarityFunc;
    }
    
    @Override
    public Join newOperator() {
        return new Join(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Join: Similarity")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Join two tables based on the string similarity of two tuples")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.JOIN_GROUP)
            .build();
    }

}
