package edu.uci.ics.textdb.dataflow.common;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;

import edu.uci.ics.textdb.api.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.api.dataflow.IPredicate;

public class DictionaryPredicate implements IPredicate {

    private Dictionary dictionary;
    private Analyzer luceneAnalyzer;
    private List<String> attributeNames;
    private KeywordMatchingType keywordMatchingType;

    /*
     * dictionary refers to list of phrases to search for. For Ex. New York if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */

    public DictionaryPredicate(Dictionary dictionary, List<String> attributeNames, Analyzer luceneAnalyzer,
            KeywordMatchingType keywordMatchingType) {

        this.dictionary = dictionary;
        this.luceneAnalyzer = luceneAnalyzer;
        this.attributeNames = attributeNames;
        this.keywordMatchingType = keywordMatchingType;
    }

    public KeywordMatchingType getKeywordMatchingType() {
        return keywordMatchingType;
    }

    /**
     * Reset the Dictionary Cursor to the beginning.
     */
    public void resetDictCursor() {
        dictionary.resetCursor();
    }

    public String getNextDictionaryEntry() {
        return dictionary.getNextValue();
    }

    public List<String> getAttributeNames() {
        return attributeNames;
    }

    public Analyzer getAnalyzer() {
        return luceneAnalyzer;
    }

}
