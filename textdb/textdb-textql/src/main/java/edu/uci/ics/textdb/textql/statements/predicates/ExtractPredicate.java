package edu.uci.ics.textdb.textql.statements.predicates;

import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * Object representation of an extraction predicate such as "KEYWORDEXTRACT(...)" predicate 
 * inside a { @code SelectExtractStatement }..
 * Subclasses have specific fields related to its extraction functionalities.
 * ExtractPredicate --+ KeywordExtractPredicate
 * 
 * @author Flavio Bayer
 * 
 */
public abstract class ExtractPredicate {
	
	/**
	 * Return the bean representation of this { @code ExtractPredicate }.
	 * @param extractionOperatorId The ID of the OperatorBean to be created.
	 */
	public abstract OperatorBean getOperatorBean(String extractionOperatorId);
	
}