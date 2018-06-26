package edu.uci.ics.texera.dataflow.keywordmatcher;

import com.fasterxml.jackson.annotation.JsonValue;

import edu.uci.ics.texera.api.exception.TexeraException;

/**
 * KeywordMatchingType: the type of keyword matching to perform. <br>
 * Currently we have 3 types of keyword matching: <br>
 * 
 * SUBSTRING_SCANBASED: <br>
 * Performs simple substring matching of the query. SubString matching is
 * case insensitive. Source tuples are provided by ScanSourceOperator. <br>
 * 
 * CONJUNCTION_INDEXBASED: <br>
 * Performs search of conjunction of query tokens. The query is tokenized
 * into keywords, with each token treated as a separate keyword. The order
 * of tokens doesn't matter in the source tuple. <br>
 * 
 * For example: <br>
 * query "book appointment" <br>
 * matches: "book appointment with the doctor" <br>
 * also matches: "an appointment to pick up a book" <br>
 * <br>
 * 
 * 
 * PHRASE_INDEXBASED: <br>
 * Performs a phrase search. The query is tokenized into keywords, with
 * stopwords treated as placeholders. The order of tokens matters in the
 * source tuple. A stopword matches an arbitary token. <br>
 * 
 * For example: <br>
 * query "book appointment" <br>
 * matches: "book appointment with the doctor" <br>
 * doesn't match: "an appointment to pick up book" <br>
 * 
 * Example of stopword as placeholders: <br>
 * query "nice a a person": matches "nice and beautiful person" <br>
 * matches "nice gentle honest person" <br>
 * doesn't match "nice person" <br>
 * doesn't match "nice gentle person" <br>
 * <br>
 * 
 * Default list of stopwords: in
 * org.apache.lucene.analysis.standard.StandardAnalyzer: <br>
 * StandardAnalyzer.STOP_WORDS_SET which includes:
 * 
 * but, be, with, such, then, for, no, will, not, are, and, their, if, this,
 * on, into, a, or, there, in, that, they, was, is, it, an, the, as, at,
 * these, by, to, of
 * 
 * @author Zuozhi Wang
 * 
 */
public enum KeywordMatchingType {
    
    SUBSTRING_SCANBASED(KeywordMatchingTypeName.SCAN),

    CONJUNCTION_INDEXBASED(KeywordMatchingTypeName.CONJUNCTION),

    PHRASE_INDEXBASED(KeywordMatchingTypeName.PHRASE),

    REGEX(KeywordMatchingTypeName.REGEX);
    
    public final String name;
    
    private KeywordMatchingType(String name) {
        this.name = name;
    }
    
    // use the name string instead of enum string in JSON
    @JsonValue
    public String getName() {
        return this.name;
    }
    
    public static KeywordMatchingType fromName(String name) {
        if (name.equalsIgnoreCase(SUBSTRING_SCANBASED.getName()) || 
                name.equalsIgnoreCase(SUBSTRING_SCANBASED.toString())) {
            return SUBSTRING_SCANBASED;
        } else if (name.equalsIgnoreCase(CONJUNCTION_INDEXBASED.getName()) || 
                name.equalsIgnoreCase(CONJUNCTION_INDEXBASED.toString())) {
            return CONJUNCTION_INDEXBASED;
        } else if (name.equalsIgnoreCase(PHRASE_INDEXBASED.getName()) || 
                name.equalsIgnoreCase(PHRASE_INDEXBASED.toString())) {
            return PHRASE_INDEXBASED;
        } else if (name.equalsIgnoreCase(REGEX.getName()) ||
                name.equalsIgnoreCase(REGEX.toString())){
            return REGEX;
        } else {
            throw new TexeraException("Cannot convert " + name + " to KeywordMatchingType");
        }
    }
    
    public class KeywordMatchingTypeName {
        public static final String SCAN = "scan";
        public static final String CONJUNCTION = "conjunction";
        public static final String PHRASE = "phrase";
        public static final String REGEX = "regex";
    }
}
