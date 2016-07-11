package edu.uci.ics.textdb.dataflow.join;

import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;

public class Join implements IOperator{
	
	private ITuple outerTuple;
	private ITuple innerTuple;
	private IPredicate joinPredicate;
	
	public Join(ITuple outerTuple, ITuple innerTuple, IPredicate joinPredicate) {
		this.outerTuple = outerTuple;
		this.innerTuple = innerTuple;
		this.joinPredicate = joinPredicate;
	}

	@Override
	public void open() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ITuple getNextTuple() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public ITuple compareAndJoin() {
		ITuple result = null;
		return result;
	}
}
