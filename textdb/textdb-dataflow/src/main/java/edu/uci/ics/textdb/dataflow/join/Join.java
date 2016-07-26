package edu.uci.ics.textdb.dataflow.join;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
/**
 * 
 * @author sripadks
 *
 */
public class Join implements IOperator{
	
	private IOperator outer;
	private IOperator inner;
	private IPredicate joinPredicate;
	
	/**
	 * This constructor is used to set the operators whose output is to be compared and joined and the 
	 * predicate which specifies the fields and constraints over which join happens.
	 * 
	 * @param outer is the outer operator producing the tuples
	 * @param inner is the inner operator producing the tuples
	 * @param joinPredicate is the predicate over which the join is made
	 */
	public Join(IOperator outer, IOperator inner, IPredicate joinPredicate) {
		this.outer = outer;
		this.inner = inner;
		this.joinPredicate = joinPredicate;
	}

	@Override
	public void open() throws Exception {
		// TODO Auto-generated method stub
		
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
		ITuple nextTuple = null;
		return nextTuple;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
}
