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

	Tuple joinTuples(Tuple innerTuple, Tuple outerTuple, Schema outputSchema)
			throws Exception;
	
	Schema generateOutputSchema(Schema innerOperatorSchema, Schema outerOperatorSchema) throws DataFlowException;
	
	String getInnerAttributeName();
	
	String getOuterAttributeName();
}