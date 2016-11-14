package edu.uci.ics.textdb.dataflow.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;

/**
 * 
 * @author sripadks
 *
 */
public class JoinDistancePredicate implements IJoinPredicate {

    private String idAttributeName;
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
     * outer and inner operators (that agree on the idAttr id attributes) and
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
     * JoinPredicate joinPre = new JoinPredicate(idAttr, reviewAttr, 10);
     * <p>
     * 
     * <p>
     * Example 1: Suppose that the outer tuple is bookTuple1 and inner tuple is
     * bookTuple2 (from two operators) and we want to join over reviewAttr the
     * words "book" and "gives". Since, both the tuples have same ID the
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
     * to be joined. Since, the tuple IDs are same, but the words are more than
     * 10 characters apart and hence join won't produce a result and simply
     * returns the tuple bookTuple1.
     * </p>
     * 
     * @param idAttribute
     *            is the ID attribute used to compare if documents are same
     * @param joinAttribute
     *            is the Attribute to perform join on
     * @param threshold
     *            is the maximum distance (in characters) between any two spans
     */
    public JoinDistancePredicate(String idAttributeName, String joinAttributeName, Integer threshold) {
        this.idAttributeName = idAttributeName;
        this.joinAttributeName = joinAttributeName;
        this.threshold = threshold;
    }

    public String getIDAttributeName() {
        return this.idAttributeName;
    }

    public String getJoinAttributeName() {
        return this.joinAttributeName;
    }

    public Integer getThreshold() {
        return this.threshold;
    }

    /**
     * This method is called by the Join operator to perform the join on the 
     * tuples passed.
     * 
     * @return New Tuple containing the result of join operation.
     */
	public ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema) throws Exception {
	    List<Span> newJoinSpanList = new ArrayList<>();

	    /*
	     * We expect the values of all fields to be the same for innerTuple and outerTuple.
	     * We only checks ID field, and field to be joined, since they are crucial to join operator.
	     * For other fields, we use the value from innerTuple.
	     * check if the ID fields are the same
	     */
	    if (! compareField(innerTuple, outerTuple, this.getIDAttributeName())) {
	        return null;
	    }
	
	    // check if the fields to be joined are the same
	    if (! compareField(innerTuple, outerTuple, this.getJoinAttributeName())) {
	        return null;
	    }

	    /*
	     * If either/both tuples have no span information, return null.
	     * Check using try/catch if both the tuples have span information.
	     * If not return null; so we can process next tuple.
	     */
	    IField spanFieldOfInnerTuple = null;
	    IField spanFieldOfOuterTuple = null;
	    try {
	        spanFieldOfInnerTuple = innerTuple.getField(SchemaConstants.SPAN_LIST);
	        spanFieldOfOuterTuple = outerTuple.getField(SchemaConstants.SPAN_LIST);
	    } catch (Exception e) {
	        return null;
	    }
	
	    List<Span> innerSpanList = null;
	    List<Span> outerSpanList = null;
	    // Check if both the fields obtained from the indexes are indeed of type
	    // ListField
	    if (spanFieldOfInnerTuple.getClass().equals(ListField.class)) {
	        innerSpanList = (List<Span>) spanFieldOfInnerTuple.getValue();
	    }
	    if (spanFieldOfOuterTuple.getClass().equals(ListField.class)) {
	        outerSpanList = (List<Span>) spanFieldOfOuterTuple.getValue();
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
	        if (!outerSpan.getFieldName().equals(this.getJoinAttributeName())) {
	            continue;
	        }
	        Iterator<Span> innerSpanIter = innerSpanList.iterator();
	        while (innerSpanIter.hasNext()) {
	            Span innerSpan = innerSpanIter.next();
	            if (!innerSpan.getFieldName().equals(this.getJoinAttributeName())) {
	                continue;
	            }
	            Integer threshold = this.getThreshold();
	            if (Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold
	                    && Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
	                Integer newSpanStartIndex = Math.min(outerSpan.getStart(), innerSpan.getStart());
	                Integer newSpanEndIndex = Math.max(outerSpan.getEnd(), innerSpan.getEnd());
	                String fieldName = this.getJoinAttributeName();
	                String fieldValue = (String) innerTuple.getField(fieldName).getValue();
	                String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
	                String spanKey = outerSpan.getKey() + "_" + innerSpan.getKey();
	                Span newSpan = new Span(fieldName, newSpanStartIndex, newSpanEndIndex, spanKey, newFieldValue);
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
	            .map(attr -> attr.getFieldName())
	            .map(fieldName -> innerTuple.getField(fieldName))
	            .collect(Collectors.toList());
	    
	    outputFields.add(new ListField<>(newJoinSpanList));
	    
	    return new DataTuple(outputSchema, outputFields.stream().toArray(IField[]::new));
	}

	/**
	 * Used to compare the value's of a field from the outer and inner tuples'.
	 * 
	 * @param innerTuple
	 * @param outerTuple
	 * @param fieldName
	 * @return True if both the tuples have the field and the values are equal.
	 */
	private boolean compareField(ITuple innerTuple, ITuple outerTuple, String fieldName) {  
	    IField innerField = innerTuple.getField(fieldName);
	    IField outerField = outerTuple.getField(fieldName);
	    
	    if (innerField == null ||  outerField == null) {
	        return false;
	    }
	
	    return innerField.getValue().equals(outerField.getValue());
	}
}
