package edu.uci.ics.textdb.dataflow.common;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.reader.DataReader;

public class DictionaryPredicate implements IPredicate {

    private IDictionary dictionary;
    private Analyzer luceneAnalyzer;
    private List<Attribute> attributeList;
    private KeywordMatchingType keywordMatchingType;

    /*
     * dictionary refers to list of phrases to search for. For Ex. New York if
     * searched in TextField, we would consider both tokens New and York; if
     * searched in String field we search for Exact string.
     */

    public DictionaryPredicate(IDictionary dictionary, List<Attribute> attributeList, Analyzer luceneAnalyzer,
            KeywordMatchingType keywordMatchingType) {

        this.dictionary = dictionary;
        this.luceneAnalyzer = luceneAnalyzer;
        this.attributeList = attributeList;
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

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

    public Analyzer getAnalyzer() {
        return luceneAnalyzer;
    }

}
