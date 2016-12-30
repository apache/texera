package edu.uci.ics.textdb.textql.languageparser;

import edu.uci.ics.textdb.web.request.beans.OperatorBean;

/**
 * ExtractPredicate class and its subclasses such as KeywordExtractPredicate.
 * Subclasses have specific fields related to its extraction functionalities.
 * ExtractPredicate --+ KeywordExtractPredicate
 * 
 * @author Flavio Bayer
 * 
 */
public abstract class ExtractPredicate {
	
	/**
	 * Return the bean representation of this { @code ExtractPredicate }.
	 * @param extractionOperatorId The ID of the created OperatorBean.
	 */
	public abstract OperatorBean getExtractOperatorBean(String extractionOperatorId);
	
	@Override
	public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        return true;
	}
}