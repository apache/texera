package edu.uci.ics.texera.dataflow.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;

import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
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

/**
 * 
 * @author sripadks
 * @author Zuozhi Wang
 *
 */
public class JoinDistancePredicate extends PredicateBase implements IJoinPredicate {

    private String joinAttributeName;
    private Integer threshold;

    /**
     * <p>
     * This constructor is used to set the parameters required for the Join
     * Operator.
     * </p>
     * 
     * <p>
     * JoinPredicate joinPre = new JoinPredicate(Attribute idAttr, Attribute
     * descriptionAttr, 10) <br>
     * will create a predicate that joins the spans of type descriptionAttr of
     * inner and outer operators (that agree on the _id attributes) and
     * outputs tuples which satisfy the criteria of being within 10 characters
     * of each other.
     * </p>
     * 
     * <p>
     * Given below is a setting and an example using this setting to use
     * JoinPredicate (consider the two tuples to be from two different
     * operators).
     * </p>
     * 
     * Setting: Consider the attributes in the schema to be idAttr (type
     * integer), authorAttr (type string), reviewAttr (type text), spanAttr
     * (type span) for a book review.
     * 
     * Let bookTuple1 be { idAttr:58, authorAttr:"Bruce Wayne", reviewAttr:"This
     * book gives us a peek into the life of Bruce Wayne when he is not fighting
     * crime as Batman.", { "book":<6, 11> } }
     * 
     * Let bookTuple2 be { idAttr:58, authorAttr:"Bruce Wayne", reviewAttr:"This
     * book gives us a peek into the life of Bruce Wayne when he is not fighting
     * crime as Batman.", { "gives":<12, 18>, "us":<>19, 22} }
     * 
     * 
     * where <spanStartIndex, spanEndIndex> represents a span.
     * 
     * JoinPredicate joinPre = new JoinPredicate(reviewAttr, 10);
     * <p>
     * 
     * <p>
     * Example 1: Suppose that the outer tuple is bookTuple1 and inner tuple is
     * bookTuple2 (from two operators) and we want to join over reviewAttr the
     * words "book" and "gives". Since, both the tuples have same _ID the
     * distance between the words are computed by using their span. Since, the
     * distance between the words (computed as |(span 1 spanStartIndex) - (span
     * 2 spanStartIndex)| and |(span 1 spanEndIndex) - (span 2 spanEndIndex)|)
     * is within 10 characters from each other, join will take place and return
     * a tuple with a span list consisting of the combined span (computed as
     * <min(span1 spanStartIndex, span2 spanStartIndex), max(span1 spanEndIndex,
     * span2 spanEndIndex)>) given by <6, 18>.
     * </p>
     * <p>
     * Example 2: Consider the previous example but with words "book" and "us"
     * to be joined. Since, the tuple _IDs are same, but the words are more than
     * 10 characters apart and hence join won't produce a result and simply
     * returns the tuple bookTuple1.
     * </p>
     *
     * @param joinAttributeName
     *            is the Attribute to perform join on
     * @param threshold
     *            is the maximum distance (in characters) between any two spans
     */
    public JoinDistancePredicate(
            String joinAttributeName, 
            Integer threshold) {
        this.joinAttributeName = joinAttributeName;
        this.threshold = threshold;
    }
    
    @JsonCreator
    public JoinDistancePredicate(
            @JsonProperty(value = PropertyNameConstants.INNER_ATTRIBUTE_NAME, required = true)
            String innerAttributeName,
            @JsonProperty(value = PropertyNameConstants.OUTER_ATTRIBUTE_NAME, required = true)
            String outerAttributeName,
            @JsonProperty(value = PropertyNameConstants.SPAN_DISTANCE, required = true)
            Integer threshold) {
        if (! innerAttributeName.equalsIgnoreCase(outerAttributeName)) {
            throw new TexeraException(PropertyNameConstants.NAME_NOT_MATCH_EXCEPTION);
        }
        this.joinAttributeName = innerAttributeName;
        this.threshold = threshold;
    }
    
    @JsonProperty(value = PropertyNameConstants.INNER_ATTRIBUTE_NAME)
    @Override
    public String getInnerAttributeName() {
        return this.joinAttributeName;
    }
    
    @JsonProperty(value = PropertyNameConstants.OUTER_ATTRIBUTE_NAME)
    @Override
    public String getOuterAttributeName() {
        return this.joinAttributeName;
    }
    
    @JsonProperty(value = PropertyNameConstants.SPAN_DISTANCE)
    public Integer getThreshold() {
        return this.threshold;
    }
    
    @Override
    public Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataflowException {
        return generateIntersectionSchema(innerOperatorSchema, outerOperatorSchema);
    }
    
    /**
     * Create outputSchema, which is the intersection of innerOperator's schema and outerOperator's schema.
     * The attributes have to be exactly the same (name and type) to be intersected.
     * 
     * InnerOperator's attributes and outerOperator's attributes must:
     * both contain the attributes to be joined.
     * both contain "_ID" attribute.
     * both contain "spanList" attribute.
     * 
     * @return outputSchema
     */
    private Schema generateIntersectionSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataflowException {
        List<Attribute> innerAttributes = innerOperatorSchema.getAttributes();
        List<Attribute> outerAttributes = outerOperatorSchema.getAttributes();
        
        List<Attribute> intersectionAttributes = 
                innerAttributes.stream()
                .filter(attr -> outerAttributes.contains(attr))
                .collect(Collectors.toList());
        
        Schema intersectionSchema = new Schema(intersectionAttributes.stream().toArray(Attribute[]::new));
        
        // check if output schema contain necessary attributes
        if (intersectionSchema.getAttributes().isEmpty()) {
            throw new DataflowException("inner operator and outer operator don't share any common attributes");
        } else if (! intersectionSchema.containsAttribute(this.joinAttributeName)) {
            throw new DataflowException("inner operator or outer operator doesn't contain join attribute");
        } else if (! intersectionSchema.containsAttribute(SchemaConstants._ID)) {
            throw new DataflowException("inner operator or outer operator doesn't contain _ID attribute");
        } else if (! intersectionSchema.containsAttribute(SchemaConstants.SPAN_LIST)) {
            throw new DataflowException("inner operator or outer operator doesn't contain spanList attribute");
        }
        
        // check if join attribute is TEXT or STRING
        AttributeType joinAttrType = intersectionSchema.getAttribute(this.joinAttributeName).getType();
        if (joinAttrType != AttributeType.TEXT && joinAttrType != AttributeType.STRING) {
            throw new DataflowException(
                    String.format("Join attribute %s must be either TEXT or STRING.", this.joinAttributeName));
        }
        
        return intersectionSchema;        
    }

    /**
     * This method is called by the Join operator to perform the join on the 
     * tuples passed.
     * 
     * @return New Tuple containing the result of join operation.
     */
    @Override
	public Tuple joinTuples(Tuple innerTuple, Tuple outerTuple, Schema outputSchema) throws Exception {
	    List<Span> newJoinSpanList = new ArrayList<>();

	    /*
	     * We expect the values of all fields to be the same for innerTuple and outerTuple.
	     * We only checks _ID field, and field to be joined, since they are crucial to join operator.
	     * For other fields, we use the value from innerTuple.
	     * check if the _ID fields are the same
	     */
	    if (! compareField(innerTuple, outerTuple, SchemaConstants._ID)) {
	        return null;
	    }
	
	    // check if the fields to be joined are the same
	    if (! compareField(innerTuple, outerTuple, this.joinAttributeName)) {
	        return null;
	    }

	    /*
	     * If either/both tuples have no span information, return null.
	     * Check using try/catch if both the tuples have span information.
	     * If not return null; so we can process next tuple.
	     */
	    ListField<Span> spanFieldOfInnerTuple = innerTuple.getField(SchemaConstants.SPAN_LIST);
	    ListField<Span> spanFieldOfOuterTuple = outerTuple.getField(SchemaConstants.SPAN_LIST);
	
	    List<Span> innerSpanList = null;
	    List<Span> outerSpanList = null;
	    // Check if both the fields obtained from the indexes are indeed of type
	    // ListField
	    if (spanFieldOfInnerTuple.getClass().equals(ListField.class)) {
	        innerSpanList = spanFieldOfInnerTuple.getValue();
	    }
	    if (spanFieldOfOuterTuple.getClass().equals(ListField.class)) {
	        outerSpanList = spanFieldOfOuterTuple.getValue();
	    }
	
	    Iterator<Span> outerSpanIter = outerSpanList.iterator();
	
	    // TODO Currently we are using two loops to go over two span lists.
	    // We can optimize it by sorting the spans based on the start position
	    // and then doing a "sort merge" of the two lists.
	    // (Also probably weed out the spans with fields that don't agree with
	    // the ones specified in the JoinPredicate during "sort merge"?)
	    while (outerSpanIter.hasNext()) {
	        Span outerSpan = outerSpanIter.next();
	        // Check if the field matches the filed over which we want to join.
	        // If not return null.
	        if (!outerSpan.getAttributeName().equals(this.joinAttributeName)) {
	            continue;
	        }
	        Iterator<Span> innerSpanIter = innerSpanList.iterator();
	        while (innerSpanIter.hasNext()) {
	            Span innerSpan = innerSpanIter.next();
	            if (!innerSpan.getAttributeName().equals(this.joinAttributeName)) {
	                continue;
	            }
	            Integer threshold = this.getThreshold();
	            if (Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold
	                    && Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
	                Integer newSpanStartIndex = Math.min(innerSpan.getStart(), outerSpan.getStart());
	                Integer newSpanEndIndex = Math.max(innerSpan.getEnd(), outerSpan.getEnd());
	                String attributeName = this.joinAttributeName;
	                String fieldValue = (String) innerTuple.getField(attributeName).getValue();
	                String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
	                String spanKey = outerSpan.getKey() + "_" + innerSpan.getKey();
	                Span newSpan = new Span(attributeName, newSpanStartIndex, newSpanEndIndex, spanKey, newFieldValue);
	                newJoinSpanList.add(newSpan);
	            }
	        }
	    }
	
	    if (newJoinSpanList.isEmpty()) {
	        return null;
	    }
	   
	    // create output fields based on innerTuple's value
	    List<Attribute> outputAttrList = outputSchema.getAttributes();
	    List<IField> outputFields = 
	            outputAttrList.stream()
	            .filter(attr -> ! attr.equals(SchemaConstants.SPAN_LIST_ATTRIBUTE))
	            .map(attr -> attr.getName())
	            .map(attributeName -> innerTuple.getField(attributeName, IField.class))
	            .collect(Collectors.toList());
	    
	    outputFields.add(new ListField<>(newJoinSpanList));
	    
	    return new Tuple(outputSchema, outputFields.stream().toArray(IField[]::new));
	}

	/**
	 * Used to compare the value's of a field from the inner and outer tuples'.
	 * 
	 * @param innerTuple
	 * @param outerTuple
	 * @param attributeName
	 * @return True if both the tuples have the field and the values are equal.
	 */
	private boolean compareField(Tuple innerTuple, Tuple outerTuple, String attributeName) {
	    IField innerField = innerTuple.getField(attributeName);
	    IField outerField = outerTuple.getField(attributeName);
	    
	    if (innerField == null ||  outerField == null) {
	        return false;
	    }
	
	    return innerField.getValue().equals(outerField.getValue());
	}
	
    @Override
    public Join newOperator() {
        return new Join(this);
    }
    
    public static Map<String, Object> getOperatorMetadata() {
    
    	// 
    	ObjectNode samplePropertyDescription = new ObjectMapper().createObjectNode();
		samplePropertyDescription.put(PropertyNameConstants.INNER_ATTRIBUTE_NAME, "Attribute that is used to join 2 tables. The"
				+ "join results will contain only the pairs that found matches based on the inner attribute on 2 different tables");
		samplePropertyDescription.put(PropertyNameConstants.OUTER_ATTRIBUTE_NAME, "Attribute that is used to join 2 tables. The"
				+ "join results will contain the pairs that matched and the table entities that do not found match from another table");
		samplePropertyDescription.put(PropertyNameConstants.SPAN_DISTANCE, "Span distance sample description");
    	
        return ImmutableMap.<String, Object>builder()
            .put(PropertyNameConstants.USER_FRIENDLY_NAME, "Join: Character Distance")
            .put(PropertyNameConstants.OPERATOR_DESCRIPTION, "Join two tables based on the character distance of two attributes")
            .put(PropertyNameConstants.OPERATOR_GROUP_NAME, OperatorGroupConstants.JOIN_GROUP)
            .put(PropertyNameConstants.PROPERTIES_DESCRIPTION, samplePropertyDescription)
            .build();
    }

}
