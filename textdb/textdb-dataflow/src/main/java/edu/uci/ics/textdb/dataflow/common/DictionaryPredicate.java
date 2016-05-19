package edu.uci.ics.textdb.dataflow.common;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IDictionary;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.SourceOperatorType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.reader.DataReader;

public class DictionaryPredicate implements IPredicate {

    private IDictionary dictionary;
    private Analyzer luceneAnalyzer;
    private List<Attribute> attributeList;
    private IDataStore dataStore;
    private SourceOperatorType srcOpType;
    
    /*
    dictionary refers to list of phrases to search for.
    For Ex. New York if searched in TextField, we would consider both tokens
    New and York; if searched in String field we search for Exact string.
     */

    public DictionaryPredicate(IDictionary dictionary, Analyzer luceneAnalyzer, List<Attribute> attributeList,
            SourceOperatorType srcOpType, IDataStore dataStore) {

        this.dictionary = dictionary;
        this.luceneAnalyzer = luceneAnalyzer;
        this.attributeList = attributeList;
        this.srcOpType = srcOpType;
        this.dataStore = dataStore;
    }

    public SourceOperatorType getSourceOperatorType() {
        return srcOpType;
    }

    public String getNextDictionaryValue() {
        return dictionary.getNextValue();
    }

    public List<Attribute> getAttributeList() {
        return attributeList;
    }

    public IDataStore getDataStore() {
        return dataStore;
    }

    public Analyzer getAnalyzer() {
        return luceneAnalyzer;
    }
    
    /*
    For the given fields , parse the query using query parser
    and generate SCAN based source operator
     */

    public IOperator getScanSourceOperator() throws ParseException, DataFlowException {
        QueryParser luceneQueryParser = new QueryParser(attributeList.get(0).getFieldName(), luceneAnalyzer);
        Query luceneQuery = luceneQueryParser.parse(DataConstants.SCAN_QUERY);
        IPredicate dataReaderPredicate = new DataReaderPredicate(dataStore, luceneQuery,DataConstants.SCAN_QUERY,luceneAnalyzer,attributeList);
        IDataReader dataReader = new DataReader(dataReaderPredicate);

        IOperator operator = new ScanBasedSourceOperator(dataReader);
        return operator;
    }
}
