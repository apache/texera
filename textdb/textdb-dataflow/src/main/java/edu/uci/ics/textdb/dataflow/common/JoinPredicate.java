package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;

public class JoinPredicate implements IPredicate {
	
	//Reserved for future
//	private enum CompOp {
//		NO_OP,	//No comparision
//		EQ_OP,	//==
//		NEQ_OP,	//!=
//		LT_OP,	//<
//		GT_OP,	//>
//		LTEQ_OP,//<=
//		GTEQ_OP	//>=
//	}
	
	private Integer outerAttributeId;
	private Attribute outerJoinAttribute;
	private Integer innerAttributeId;
	private Attribute innerJoinAttribute;
	private Integer threshold;
	
	public JoinPredicate(Integer outerAttributeId, Attribute outerJoinAttribute, Integer innerAttributeId, Attribute innerJoinAttribute, Integer threshold) {
		this.outerAttributeId = outerAttributeId;
		this.outerJoinAttribute = outerJoinAttribute;
		this.innerAttributeId = innerAttributeId;
		this.innerJoinAttribute = innerJoinAttribute;
		this.threshold = threshold;
	}
	
	public Integer getOuterAttrId() {
		return outerAttributeId;
	}
	
	public Integer getInnerAttrId() {
		return innerAttributeId;
	}
	
	public Attribute getOuterAttr() {
		return outerJoinAttribute;
	}
	
	public Attribute getInnerAttr() {
		return innerJoinAttribute;
	}
	
	public Integer getThreshold() {
		return threshold;
	}
}
