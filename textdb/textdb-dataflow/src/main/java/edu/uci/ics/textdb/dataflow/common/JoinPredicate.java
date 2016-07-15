package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;

/**
 * 
 * @author sripadks
 *
 */
public class JoinPredicate implements IPredicate {
	
	private Attribute idAttribute;
	private Attribute joinAttribute;
	private Integer threshold;
	
	/**
	 * <p>This constructor is used to set the parameters required for the Join 
	 * Operator.</p>
	 * 
	 * <p>
	 * JoinPredicate joinPre = new JoinPredicate(Attribute idAttr, 
	 * Attribute descriptionAttr, 10)
	 * <br>will create a predicate that joins the spans of type 
	 * descriptionAttr of outer and inner operators (that agree on the idAttr 
	 * id attributes) and outputs tuples which satisfy the criteria of being
	 * within 10 characters of each other. </p>
	 * 
	 * <p>
	 * Given below is a setting and an example using this setting to use 
	 * JoinPredicate (consider the two tuples to be from two different 
	 * operators).</p>
	 * 
	 * Setting:
	 * Consider the attributes in the schema to be idAttr (type integer),  
	 * authorAttr (type string), reviewAttr (type text), spanAttr (type span) 
	 * for a book review.
	 * 
	 * Let bookTuple1 be { idAttr:58, authorAttr:"Bruce Wayne", reviewAttr:"This
	 *  book gives us a peek into the life of Bruce Wayne when he is not 
	 *  fighting crime as Batman.", { "book":<6, 11> } }
	 *  
	 * Let bookTuple2 be { idAttr:58, authorAttr:"Bruce Wayne", reviewAttr:"This
	 *  book gives us a peek into the life of Bruce Wayne when he is not 
	 *  fighting crime as Batman.", { "gives":<12, 18>, "us":<>19, 22} }
	 * 
	 * 
	 * where <spanStartIndex, spanEndIndex> represents a span.
	 * 
	 * JoinPredicate joinPre = new JoinPredicate(idAttr, reviewAttr, 10);
	 * <p>
	 * 
	 * <p>
	 * Example 1:
	 * Suppose that the outer tuple is bookTuple1 and inner tuple is bookTuple2
	 *  (from two operators) and we want to join over reviewAttr the words 
	 *  "book" and "gives". Since, both the tuples have same ID the distance 
	 *  between the words are computed by using their span. Since, the distance
	 *  between the words (computed as |(span 1 spanStartIndex) - 
	 *  (span 2 spanStartIndex)| and |(span 1 spanEndIndex) - 
	 *  (span 2 spanEndIndex)|) is within 10 characters from each other, join 
	 *  will take place and return a tuple with a span list consisting of the 
	 *  combined span (computed as <min(span1 spanStartIndex, span2 spanStartIndex),
	 *   max(span1 spanEndIndex, span2 spanEndIndex)>) given by <6, 18>.
	 * </p>
	 * <p>
	 * Example 2:
	 * Consider the previous example but with words "book" and "us" to be 
	 * joined. Since, the tuple IDs are same, but the words are more than 10 
	 * characters apart and hence join won't produce a result and simply returns
	 *  the tuple bookTuple1. </p>
	 * 
	 * @param idAttribute is the ID attribute used to compare if documents are same
	 * @param joinAttribute is the Attribute to perform join on
	 * @param threshold is the maximum distance (in characters) between any two
	 *  spans
	 */
	public JoinPredicate(Attribute idAttribute, Attribute joinAttribute, 
			Integer threshold) {
		this.idAttribute = idAttribute;
		this.joinAttribute = joinAttribute;
		this.threshold = threshold;
	}
	
	public Attribute getidAttribute() {
		return this.idAttribute;
	}
	
	
	public Attribute getjoinAttribute() {
		return this.joinAttribute;
	}
	
	public Integer getThreshold() {
		return this.threshold;
	}
}
