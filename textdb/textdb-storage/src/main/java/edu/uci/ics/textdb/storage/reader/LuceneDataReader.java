/**
 * 
 */
package edu.uci.ics.textdb.storage.reader;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.api.storage.IDataStore;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;

/**
 * @author sandeepreddy602
 *
 */
public class LuceneDataReader implements IDataReader{
    
    private IDataStore dataStore;
    private int cursor = -1;
    private IndexSearcher indexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader indexReader;
    private Query query;
    
    public LuceneDataReader(IDataStore dataStore, Query query) {
        this.dataStore = dataStore;
        this.query = query;
    }
    
    @Override
    public void open() throws DataFlowException {


        try {
            Directory directory = FSDirectory.open(Paths.get(dataStore.getDataDirectory()));
            indexReader = DirectoryReader.open(directory);
                		
            indexSearcher = new IndexSearcher(indexReader);
            TopDocs topDocs = indexSearcher.search(query, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = OPENED;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }              
    }

    @Override
    public ITuple getNextTuple() throws DataFlowException {
        if(cursor == CLOSED){
            throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
        }
        try {
            if(cursor >= scoreDocs.length){
                return null;
            }
            Document document = indexSearcher.doc(scoreDocs[cursor++].doc);
            
            List<IField> fields = new ArrayList<IField>();
            for (Attribute  attr : dataStore.getSchema().getAttributes()) {
                FieldType fieldType = attr.getFieldType();
                String fieldValue = document.get(attr.getFieldName());
                fields.add(Utils.getField(fieldType, fieldValue));
            }
            
            DataTuple dataTuple = new DataTuple(dataStore.getSchema(), fields.toArray(new IField[fields.size()]));
            return dataTuple;
        } catch (IOException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
        
    }

    @Override
    public void close() throws Exception {
        cursor = CLOSED;
        if(indexReader != null){
            try {
                indexReader.close();
                indexReader = null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
        }
    }
    
}
