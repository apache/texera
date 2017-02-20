package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.DataFlowException;

/**
 * IJoinOperator is the interface for the classes implementing predicates for 
 * the Join Operator.
 * 
 * @author sripadks
 */
public interface IJoinPredicate {

	ITuple joinTuples(ITuple outerTuple, ITuple innerTuple, Schema outputSchema)
			throws Exception;
	
	Schema generateOutputSchema(Schema outerOperatorSchema, Schema innerOperatorSchema) throws DataFlowException;
	
	String getInnerAttributeName();
	
	String getOuterAttributeName();
}