package edu.uci.ics.textdb.dataflow.common;

import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.SourceOperatorType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.dictionarymatcher.Dictionary;
import edu.uci.ics.textdb.dataflow.source.ScanBasedSourceOperator;
import edu.uci.ics.textdb.storage.DataReaderPredicate;
import edu.uci.ics.textdb.storage.reader.DataReader;

public class DictionaryPredicate implements IPredicate {

    private Dictionary dictionary;
    private Analyzer analyzer;
    private List<Attribute> attributeList;
    private IDataStore dataStore;
    private SourceOperatorType srcOpType;

    public DictionaryPredicate(Dictionary dictionary, Analyzer analyzer, List<Attribute> attributeList,
            SourceOperatorType srcOpType, IDataStore dataStore) {

        this.dictionary = dictionary;
        this.analyzer = analyzer;
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
        return analyzer;
    }

    public IOperator getSourceOperator() throws ParseException, DataFlowException {
        QueryParser luceneQueryParser = new QueryParser(attributeList.get(0).getFieldName(), analyzer);
        Query luceneQuery = luceneQueryParser.parse(DataConstants.SCAN_QUERY);
        IPredicate dataReaderPredicate = new DataReaderPredicate(dataStore, luceneQuery);
        IDataReader dataReader = new DataReader(dataReaderPredicate);

        IOperator operator = new ScanBasedSourceOperator(dataReader);
        return operator;
    }
}
