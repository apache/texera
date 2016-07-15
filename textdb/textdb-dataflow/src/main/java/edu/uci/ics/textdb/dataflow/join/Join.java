package edu.uci.ics.textdb.dataflow.join;

import java.util.Iterator;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;
/**
 * 
 * @author sripadks
 *
 */
public class Join implements IOperator{
	
	private IOperator outerOperator;
	private IOperator innerOperator;
	private IPredicate joinPredicate;
	private boolean getNextOuterTuple = true;
	private ITuple outerTuple;
	private ITuple innerTuple;
	
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
		this.joinPredicate = joinPredicate;
	}

	@Override
	public void open() throws DataFlowException {
		// TODO Auto-generated method stub
		try {
			outerOperator.open();
			innerOperator.open();
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
		// TODO Auto-generated method stub
		if(getNextOuterTuple == true) {
			if((outerTuple = outerOperator.getNextTuple()) != null) {
				;
			}
		} else {
			
		}
		ITuple nextTuple = null;
		return nextTuple;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
