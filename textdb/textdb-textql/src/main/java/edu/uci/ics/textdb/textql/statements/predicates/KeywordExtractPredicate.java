package edu.uci.ics.textdb.textql.statements.predicates;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;

/**
 * Object representation of a "KEYWORDEXTRACT(...)" predicate inside a { @code SelectExtractStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicate extends ExtractPredicate {
    
    /**
     * The { @link List } of fields on which the keyword search should be performed.
     */
    private List<String> matchingFields;
    
    /**
     * The keyword(s) used for a keyword search.
     */
    private String keywords;
    
    /**
     * The type of matching to be performed during the keyword search.
     */ 
    private String matchingType;
    
    
    /**
     * Create a { @code KeywordExtractPredicate } with all the parameters set to { @code null }.
     * @param id The id of this statement.
     */
    public KeywordExtractPredicate() {
      this(null, null, null);
    }

    /**
     * Create a { @code KeywordExtractPredicate } with the given parameters.
     * @param matchingFields List of fields to extract information from.
     * @param keywords The keywords to look for during extraction.
     * @param matchingType The string representation of the { @code KeywordMatchingType } used for extraction.
     */
    public KeywordExtractPredicate(List<String> matchingFields, String keywords, String matchingType) {
        this.matchingFields = matchingFields;
        this.keywords = keywords;
        this.matchingType = matchingType;
    }
    
    
    /**
     * Get the list of names of fields to be matched during extraction.
     * @return The list of names of fields to be matched during extraction.
     */
    public List<String> getMatchingFields() {
        return matchingFields;
    }

    /**
     * Set the list of names of fields to be matched during extraction.
     * @param matchingFields The list of names of fields to be matched during extraction.
     */
    public void setMatchingFields(List<String> matchingFields) {
        this.matchingFields = matchingFields;
    }

    /**
     * Get the keyword(s) to look for during extraction.
     * @return The keyword(s) to look for during extraction.
     */
    public String getKeywords() {
        return keywords;
    }

    /**
     * Set the keyword(s) to look for during extraction.
     * @param keywords The keyword(s) to look for during extraction.
     */
    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    /**
     * Get the matching type of the extraction.
     * @return The matching type of the extraction.
     */
    public String getMatchingType() {
        return matchingType;
    }

    /**
     * Set the matching type of the extraction.
     * @param matchingType The matching type of the extraction.
     */
    public void setMatchingType(String matchingType) {
        this.matchingType = matchingType;
    }
    

    @Override
    public boolean equals(Object other) {
        if (other == null) { return false; }
        if (other.getClass() != getClass()) { return false; }
        KeywordExtractPredicate keywordExtractPredicate = (KeywordExtractPredicate) other;
        return new EqualsBuilder()
                .appendSuper(super.equals(keywordExtractPredicate))
                .append(matchingFields, keywordExtractPredicate.matchingFields)
                .append(keywords, keywordExtractPredicate.keywords)
                .append(matchingType, keywordExtractPredicate.matchingType)
                .isEquals();
    }
    
}