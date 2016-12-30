package edu.uci.ics.textdb.textql.languageparser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;

import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.web.request.beans.KeywordMatcherBean;

/**
 * Object representation of a parsed "KEYWORDEXTRACT(..)" predicate inside a { @code SelectStatement }.
 * 
 * @author Flavio Bayer
 *
 */
public class KeywordExtractPredicate extends ExtractPredicate {
    
    /**
     * The { @link List } of fields on which the keyword search should be performed.
     */
    public List<String> matchingFields;
    
    /**
     * The keyword(s) used for a keyword search.
     */
    public String keywords;
    
    /**
     * The type of matching to be performed during the keyword search.
     */ 
    public String matchingType;
    
    /**
     * The default { @code KeywordMatchingType } to be returned as matching type if the current matching type
     * of the statement is set to { @code null }.
     */
    public static final KeywordMatchingType DEFAULT_MATCH_TYPE = KeywordMatchingType.CONJUNCTION_INDEXBASED;
    
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
     * Return this operator converted to a { @code KeywordMatcherBean }.
     * @param extractionOperatorId The ID of the created OperatorBean.
     */
    @Override
    public KeywordMatcherBean getExtractOperatorBean(String extractionOperatorId) {
        KeywordMatchingType keywordMatchingType = DEFAULT_MATCH_TYPE;
        if(this.matchingType!=null){
            Map<String, KeywordMatchingType> keywordMatchingTypeAlias = new HashMap<String, KeywordMatchingType>();
            keywordMatchingTypeAlias.put("conjunction", KeywordMatchingType.CONJUNCTION_INDEXBASED);
            keywordMatchingTypeAlias.put("phrase", KeywordMatchingType.PHRASE_INDEXBASED);
            keywordMatchingTypeAlias.put("substring", KeywordMatchingType.SUBSTRING_SCANBASED);
            keywordMatchingType = keywordMatchingTypeAlias.get(this.matchingType.toLowerCase());
        }
        return new KeywordMatcherBean(extractionOperatorId, "KeywordMatcher", String.join(",", this.matchingFields), null, 
                    null, this.keywords, keywordMatchingType);
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