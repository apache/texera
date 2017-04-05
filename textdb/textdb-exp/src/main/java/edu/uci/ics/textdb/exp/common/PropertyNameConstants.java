package edu.uci.ics.textdb.exp.common;

/**
 * PropertyNameConstants defines the key names 
 *   in the JSON representation of each operator.
 * 
 * @author Zuozhi Wang
 *
 */
public class PropertyNameConstants {
    
    private PropertyNameConstants() {};
    
    // common property names
    public static final String ATTRIBUTE_NAMES = "attributes";
    public static final String LUCENE_ANALYZER_STRING = "lucene_analzer";
    public static final String SPAN_LIST_NAME = "span_list_name";
    public static final String TABLE_NAME = "data_source";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";
    
    // related to keyword matcher
    public static final String KEYWORD_QUERY = "query";
    public static final String KEYWORD_MATCHING_TYPE = "matching_type";
    
    // related to dictionary matcher
    public static final String DICTIONARY = "dictionary";
    
    // related to regex matcher
    public static final String REGEX_QUERY = "regex";
    
    // related to fuzzy token matcher
    public static final String FUZZY_TOKEN_QUERY = "query";

}
