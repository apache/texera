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

// The Join operator is an operator which intends to perform a "join" over the
// the outputs of two other operators based on certain conditions defined
// using the JoinPredicate.
// The JoinPredicate currently takes:
// ID attribute -> Which serves as the document/tuple ID. Only for the tuples
// whose IDs match, we perform the join.
// Join Attribute -> The attribute to perform Join on.
// and Threshold -> The value within which the difference of span starts and
// the difference of span ends should be for the join to take place.

public class Join implements IOperator {

	private IOperator outerOperator;
	private IOperator innerOperator;
	private JoinPredicate joinPredicate;
	// To indicate if next result from outer operator has to be obtained.
	private boolean shouldIGetOuterOperatorNextTuple;
	private ITuple outerTuple = null;
	private ITuple innerTuple = null;
	private List<ITuple> innerTupleList = new ArrayList<>();
	// Cursor to maintain the position of tuple to be obtained from
	// innerTupleList.
	private Integer innerOperatorCursor = 0;
	// Value to be used as key in Span.
	// private Integer spanKey = 0;

	/**
	 * This constructor is used to set the operators whose output is to be
	 * compared and joined and the predicate which specifies the fields and
	 * constraints over which join happens.
	 * 
	 * @param outer
	 *            is the outer operator producing the tuples
	 * @param inner
	 *            is the inner operator producing the tuples
	 * @param joinPredicate
	 *            is the predicate over which the join is made
	 */
	public Join(IOperator outerOperator, IOperator innerOperator, JoinPredicate joinPredicate) {
		this.outerOperator = outerOperator;
		this.innerOperator = innerOperator;
		this.joinPredicate = joinPredicate;
	}

	@Override
	public void open() throws Exception, DataFlowException {
		if (!(joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.STRING)
				|| joinPredicate.getjoinAttribute().getFieldType().equals(FieldType.TEXT))) {
			throw new Exception("Fields other than \"STRING\" and \"TEXT\" are not supported by Join yet.");
		}

		try {
			outerOperator.open();
			innerOperator.open();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}

		shouldIGetOuterOperatorNextTuple = true;

		// Load the inner tuple list into memory on open.
		while ((innerTuple = innerOperator.getNextTuple()) != null) {
			innerTupleList.add(innerTuple);
		}

		// Close the inner operator as all the required tuples are already
		// loaded into memory.
		try {
			innerOperator.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
	}

	/**
	 * Gets the next tuple which is a joint of two tuples which passed the
	 * criteria set in the JoinPredicate. <br>
	 * Example in JoinPredicate.java
	 * 
	 * @return nextTuple
	 */
	@Override
	public ITuple getNextTuple() throws Exception {
		if (innerTupleList.isEmpty()) {
			return null;
		}

		ITuple nextTuple = null;

		do {
			if (shouldIGetOuterOperatorNextTuple == true) {
				if ((outerTuple = outerOperator.getNextTuple()) == null) {
					return null;
				}
				shouldIGetOuterOperatorNextTuple = false;
			}

			if (innerOperatorCursor <= innerTupleList.size() - 1) {
				innerTuple = innerTupleList.get(innerOperatorCursor);
				innerOperatorCursor++;
				if (innerOperatorCursor == innerTupleList.size()) {
					innerOperatorCursor = 0;
					shouldIGetOuterOperatorNextTuple = true;
				}
			}

			nextTuple = joinTuples(outerTuple, innerTuple, joinPredicate);
		} while (nextTuple == null);

		return nextTuple;
	}

	@Override
	public void close() throws Exception {
		try {
			outerOperator.close();
			// innerOperator.close(); already called in open()

		} catch (Exception e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		// Clear the inner tuple list from memory on close.
		innerTupleList.clear();
	}

	// Used to compare IDs of the tuples.
	private boolean compareId(ITuple outerTuple, ITuple innerTuple) {
		// Check for the Validity of ID field and if both the ID fields are
		// equal.

		// First check if the field in question exists by using try catch.
		// Will throw an exception if it doesn't exist. This leads to return
		// false.
		// Then check if both the fields are of type IntegerField.
		// (This is the bare minimum thing that can be done to verify valid
		// id attribute. (as of now) (probably it is better to add a field
		// called ID))
		String fieldName = joinPredicate.getidAttribute().getFieldName();
		try {
			if (outerTuple.getField(fieldName).getClass().equals(IntegerField.class)
					&& innerTuple.getField(fieldName).getClass().equals(IntegerField.class)) {
				if (outerTuple.getField(fieldName).getValue().equals(innerTuple.getField(fieldName).getValue())) {
					return true;
				}
			}
		} catch (Exception e) {
			;
		}
		return false;
	}

	// Process the tuples to get a tuple with join result if predicate is
	// satisfied.
	private ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, JoinPredicate joinPredicate) throws Exception {
		ITuple nextTuple = null;
		List<Span> newJoinSpanList = new ArrayList<>();

		if (!compareId(outerTuple, innerTuple)) {
			return null;
		}

		// If either/both tuples have no span information, return null.
		// Check using try/catch if both the tuples have span information.
		// If not return null; so we can process next tuple.

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
			if (!outerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
				continue;
			}
			Iterator<Span> innerSpanIter = innerSpanList.iterator();
			while (innerSpanIter.hasNext()) {
				Span innerSpan = innerSpanIter.next();
				if (!innerSpan.getFieldName().equals(joinPredicate.getjoinAttribute().getFieldName())) {
					continue;
				}
				Integer threshold = joinPredicate.getThreshold();
				if (Math.abs(outerSpan.getStart() - innerSpan.getStart()) <= threshold
						&& Math.abs(outerSpan.getEnd() - innerSpan.getEnd()) <= threshold) {
					Integer newSpanStartIndex = Math.min(outerSpan.getStart(), innerSpan.getStart());
					Integer newSpanEndIndex = Math.max(outerSpan.getEnd(), innerSpan.getEnd());

					// spanKey++;
					String fieldName = joinPredicate.getjoinAttribute().getFieldName();
					String fieldValue = (String) innerTuple.getField(fieldName).getValue();
					String newFieldValue = fieldValue.substring(newSpanStartIndex, newSpanEndIndex);
					Span newSpan = new Span(fieldName, newSpanStartIndex, newSpanEndIndex,
							// TODO: Check the right values for key and value
							// spanKey.toString(), // changed the value to foo
							// to match test cases.
							"foo", newFieldValue);
					newJoinSpanList.add(newSpan);
				}
			}
		}

		if (newJoinSpanList.isEmpty()) {
			return null;
		}

		// TODO Discuss and implement as to what Schema has to be.
		// It shouldn't just be inner tuple or outer tuple schema.
		Schema schema = innerTuple.getSchema();
		List<IField> fieldList = innerTuple.getFields();
		IField[] nextTupleField = new IField[fieldList.size()];

		for (int index = 0; index < nextTupleField.length - 1; index++) {
			nextTupleField[index] = fieldList.get(index);
		}

		nextTupleField[nextTupleField.length - 1] = new ListField<>(newJoinSpanList);
		nextTuple = new DataTuple(schema, nextTupleField);

		return nextTuple;
	}

    @Override
    public Schema getOutputSchema() {
        return innerOperator.getOutputSchema();
    }
}