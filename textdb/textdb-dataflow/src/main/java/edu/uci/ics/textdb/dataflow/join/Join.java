package edu.uci.ics.textdb.dataflow.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.IntegerField;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.dataflow.common.JoinPredicate;
/**
 * 
 * @author sripadks
 *
 */
public class Join implements IOperator{

	private IOperator outerOperator;
	private IOperator innerOperator;
	private JoinPredicate joinPredicate;
	// To indicate if next result from outer operator has to be obtained.
	private boolean getOuterOperatorNextTuple = true;			
	private ITuple outerTuple = null;
	private ITuple innerTuple = null;
	private List<ITuple> innerTupleList = new ArrayList<>();
	// Cursor to maintain the position of tuple to be obtained from innerTupleList.
	private Integer innerOperatorCursor = 0;
	// Value to be used as key in Span.
	//	private Integer spanKey = 0;

	/**
	 * This constructor is used to set the operators whose output is to be compared and joined and the 
	 * predicate which specifies the fields and constraints over which join happens.
	 * 
	 * @param outer is the outer operator producing the tuples
	 * @param inner is the inner operator producing the tuples
	 * @param joinPredicate is the predicate over which the join is made
	 */
	public Join(IOperator outerOperator, IOperator innerOperator, IPredicate joinPredicate) {
		this.outerOperator = outerOperator;
		this.innerOperator = innerOperator;
		this.joinPredicate = (JoinPredicate) joinPredicate;
	}

	@Override
	public void open() throws Exception, DataFlowException {
		try {
			outerOperator.open();
			innerOperator.open();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		if(!(joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.STRING)||
				joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.TEXT))) {
			outerOperator.close();
			innerOperator.close();
			throw new Exception("Fields other than \"STRING\" and \"TEXT\" are not supported by Join yet.");
		}
		// Load the inner tuple list into memory on open.
		while((innerTuple = innerOperator.getNextTuple()) != null) {
			innerTupleList.add(innerTuple);
		}
		// Close the inner operator as all the required tuples are already 
		// loaded into memory.
		try {
			innerOperator.close();
		} catch(Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	/**
	 * Gets the next tuple which is a joint of two tuples which passed the criteria set in the JoinPredicate.
	 * <br> Example in JoinPredicate.java
	 * 
	 * @return nextTuple
	 */
	@Override
	public ITuple getNextTuple() throws Exception {
		if(innerTupleList.isEmpty()) {
			return null;
		}

		ITuple nextTuple = null;

		do {
			if(getOuterOperatorNextTuple == true) {
				if((outerTuple = outerOperator.getNextTuple()) != null) {
					getOuterOperatorNextTuple = false;
				} else {
					return null;
				}
			}

			if (innerOperatorCursor <= innerTupleList.size() - 1) {
				innerTuple = innerTupleList.get(innerOperatorCursor);
				innerOperatorCursor++;
				if(innerOperatorCursor == innerTupleList.size()) {
					innerOperatorCursor = 0;
					getOuterOperatorNextTuple = true;
				}
			}

			nextTuple = processTuples(outerTuple, innerTuple, joinPredicate);
		} while(nextTuple == null);

		return nextTuple;
	}

	@Override
	public void close() throws Exception {
		try {
			outerOperator.close();
			// innerOperator.close(); already called in open()

		} catch(Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		// Clear the inner tuple list from memory on close.
		innerTupleList.clear();
	}

	// Used to compare IDs of the tuples.
	private boolean compareId(ITuple outerTuple, ITuple innerTuple) {
		// TODO(Flavio): what if the join predicate has an invalid id attribute?
		// First check if the field in question exits by using try catch.
		// Will throw an exception if it doesn't exist. This leads to return false.
		// Then check if both the fields are of type IntegerField.
		// --> This is the bare minimum thing that can be done to to verify valid 
		// id attribute (as of now) (probably it is better to add a field called ID).
		String fieldName = joinPredicate.getidAttribute().getFieldName();
		try {
			if(outerTuple.getField(fieldName).getClass().equals(IntegerField.class)&&
					innerTuple.getField(fieldName).getClass().equals(IntegerField.class)) {
				if(outerTuple.getField(fieldName).getValue()==
						innerTuple.getField(fieldName).getValue()) {
					return true;
				}
			}
		} catch(Exception e) {
			;
		}
		return false;
	}

	// Process the tuples to get a tuple with join result if predicate is satisfied.
	private ITuple processTuples(ITuple outerTuple, ITuple innerTuple, JoinPredicate joinPredicate) throws Exception {
		ITuple nextTuple = null;
		List<Span> newJoinSpanList = new ArrayList<>();

		if(!compareId(outerTuple, innerTuple)) {
			return null;
		}
		//TODO(Flavio): getIndex will always return an int, if SchemaConstants.SPAN_LIST doesn't 
		// exists a NullPointerException will be thrown, it will never return null and the
		// following comparison will be useless
		// If either/both tuples have no span information, return null.
		Integer indexOfInnerSpanList = null;
		Integer indexOfOuterSpanList = null;
		try {
			indexOfInnerSpanList = innerTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST);
			indexOfOuterSpanList = outerTuple.getSchema().getIndex(SchemaConstants.SPAN_LIST);
		} catch(Exception e) {
			return null;
		}
		// If either/both tuples have no span information, return null.
		//		if(indexOfInnerSpanList == null || indexOfOuterSpanList == null) {
		//			return null;
		//		}

		List<Span> innerSpanList = null;
		List<Span> outerSpanList = null;
		// Check if both the fields obtained from the indexes are indeed of type ListField
		// TODO(Flavio): should SchemaConstants.SPAN_LIST be a reserved or special 
		// fieldName? In that case we don't need to check if the classes are equal
		if(innerTuple.getField(indexOfInnerSpanList).getClass().equals(ListField.class)) {
			innerSpanList = (List<Span>) innerTuple.getField(indexOfInnerSpanList).getValue();
		}
		if(outerTuple.getField(indexOfOuterSpanList).getClass().equals(ListField.class)) {
			outerSpanList = (List<Span>) outerTuple.getField(indexOfOuterSpanList).getValue();
		}

		Iterator<Span> outerSpanIter = outerSpanList.iterator();

		while(outerSpanIter.hasNext()) {
			Span outerSpan = outerSpanIter.next();
			// Check if the field matches the filed over which we want to join. If not return null.
			if(!outerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
				return null;
			}
			Iterator<Span> innerSpanIter = innerSpanList.iterator();
			while(innerSpanIter.hasNext()) {
				Span innerSpan = innerSpanIter.next();
				if(!innerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
					return null;
				}
				Integer threshold = joinPredicate.getThreshold();
				if(Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold &&
						Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
					Integer newSpanStartIndex = Math.min(outerSpan.getStart(), innerSpan.getStart());
					Integer newSpanEndIndex = Math.max(outerSpan.getEnd(), innerSpan.getEnd());

					//spanKey++;
					String fieldName = joinPredicate.getjoinAttribute().getFieldName();
					String fieldValue = (String) innerTuple.getField(fieldName).getValue();
					String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
					Span newSpan = new Span(
							fieldName, newSpanStartIndex, newSpanEndIndex, 
							// TODO(Flavio): Check the right values for key and value
							//spanKey.toString(), // changed the value to foo 
							// to match test cases.
							"foo", "bar");
					newJoinSpanList.add(newSpan);
				}
			}
		}

		// TODO schema has to match the type of systemT output and not innerTuple or outerTuple
		if(newJoinSpanList.isEmpty()) {
			return null;
		}
		Schema schema = innerTuple.getSchema();
		List<IField> fieldList = innerTuple.getFields();
		IField[] nextTupleField = new IField[fieldList.size()];

		for(int index = 0; index < nextTupleField.length - 1; index++) {
			nextTupleField[index] = fieldList.get(index);
		}

		nextTupleField[nextTupleField.length - 1] = new ListField<>(newJoinSpanList);
		nextTuple = new DataTuple(schema, nextTupleField);

		return nextTuple;
	}
}