package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.schema.Schema;
import edu.uci.ics.textdb.api.tuple.Tuple;

/**
 * IJoinOperator is the interface for the classes implementing predicates for 
 * the Join Operator.
 * 
 * @author sripadks
 */
public interface IJoinPredicate {

	Tuple joinTuples(Tuple outerTuple, Tuple innerTuple, Schema outputSchema)
			throws Exception;
	
	Schema generateOutputSchema(Schema outerOperatorSchema, Schema innerOperatorSchema) throws DataFlowException;
	
	String getInnerAttributeName();
	
	String getOuterAttributeName();
}