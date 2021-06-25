package edu.uci.ics.texera.dataflow.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.ISourceOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IDField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.storage.DataReader;
import edu.uci.ics.texera.storage.RelationManager;

/**
 * @author Qinhua Huang
 */
public class WordCountIndexSource implements ISourceOperator {
    
    public static final String WORD = "word";
    public static final String COUNT = "count";
    public static final Attribute WORD_ATTR = new Attribute(WORD, AttributeType.STRING);
    public static final Attribute COUNT_ATTR = new Attribute(COUNT, AttributeType.INTEGER);
    public static final Schema SCHEMA_WORD_COUNT = new Schema(SchemaConstants._ID_ATTRIBUTE, WORD_ATTR, COUNT_ATTR);
    
    private WordCountIndexSourcePredicate predicate;
    private int cursor = CLOSED;
    
    private List<Entry<String, Integer>> sortedWordCountMap;
    private Iterator<Entry<String, Integer>> wordCountIterator;
    
    public WordCountIndexSource(WordCountIndexSourcePredicate predicate) {
        this.predicate = predicate;
    }
    
    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
            return;
        }
        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws TexeraException {
        if (cursor == CLOSED) {
            throw new DataflowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        return computeNextMatchingTuple();
    }

    private Tuple computeNextMatchingTuple() throws TexeraException {
        if (sortedWordCountMap == null) {
            computeWordCount();
        }
        if (wordCountIterator.hasNext()) {
            Entry<String, Integer> entry = wordCountIterator.next();
            List<IField> tupleFieldList = new ArrayList<>();
            // Generate the new UUID.
            tupleFieldList.add(IDField.newRandomID());
            tupleFieldList.add(new StringField(entry.getKey()));
            tupleFieldList.add(new IntegerField(entry.getValue()));
            
            cursor++;
            return new Tuple(SCHEMA_WORD_COUNT, tupleFieldList);
        }
        return null;
    }
    
    private void computeWordCount() throws TexeraException {
        try {
            HashMap<String, Integer> wordCountMap = new HashMap<>();
            DataReader dataReader = RelationManager.getInstance().getTableDataReader(
                    predicate.getTableName(), new MatchAllDocsQuery());
            
            dataReader.open();
            
            IndexReader luceneIndexReader = dataReader.getLuceneIndexReader();
            
            for (int i = 0; i< luceneIndexReader.numDocs(); i++) {
                Terms termVector = luceneIndexReader.getTermVector(i, predicate.getAttribute());
                
                TermsEnum termsEnum = termVector.iterator();
                while(termsEnum.next() != null){
                    String key = termsEnum.term().utf8ToString();
                    wordCountMap.put(key, wordCountMap.get(key)==null ?
                            ((int) termsEnum.totalTermFreq()) :
                                wordCountMap.get(key) + ((int) termsEnum.totalTermFreq()));
                }
            }
            
            luceneIndexReader.close();
            dataReader.close();
            
            sortedWordCountMap = wordCountMap.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                    .collect(Collectors.toList());
            wordCountIterator = sortedWordCountMap.iterator();
            
        } catch (IOException e) {
            throw new DataflowException(e);
        }
        
    }
    

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return;
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return SCHEMA_WORD_COUNT;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_INPUT_SCHEMA_FOR_SOURCE);
    }
}
