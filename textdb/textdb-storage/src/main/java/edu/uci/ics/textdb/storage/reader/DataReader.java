/**
 * 
 */
package edu.uci.ics.textdb.storage.reader;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.textdb.common.field.Span;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * @author sandeepreddy602
 *
 */
public class DataReader implements IDataReader{

    private int cursor = CLOSED;
    private IndexSearcher luceneIndexSearcher;
    private ScoreDoc[] scoreDocs;
    private IndexReader luceneIndexReader;
    private DataReaderPredicate dataReaderPredicate;
    private ArrayList<String> queryTokens;
    private List<Attribute> attributeList;
    private List<BytesRef> queryTokensInBytesRef;
    // The schema of the data tuple
    private Schema schema;
    //The schema of the data tuple along with the span information.
    private Schema spanSchema;

    public DataReader(IPredicate dataReaderPredicate) {
        this.dataReaderPredicate = (DataReaderPredicate)dataReaderPredicate;
    }
    
    @Override
    public void open() throws DataFlowException {


        try {
            String dataDirectory = dataReaderPredicate.getDataStore().getDataDirectory();
            Directory directory = FSDirectory.open(Paths.get(dataDirectory));
            luceneIndexReader = DirectoryReader.open(directory);
                		
            luceneIndexSearcher = new IndexSearcher(luceneIndexReader);
            TopDocs topDocs = luceneIndexSearcher.search(dataReaderPredicate.getLuceneQuery(), Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;
            cursor = OPENED;

            this.queryTokens = Utils.tokenizeQuery(dataReaderPredicate.getLuceneAnalyzer(),dataReaderPredicate.getQueryString());

            // sort the query tokens, as the term vector are also sorted.
            // This makes the seek faster.
            this.queryTokens.sort(String.CASE_INSENSITIVE_ORDER);

            // The terms in the term vector are stored as ByteRef,
            // hence convert token from String format to ByteRef and then search.

            this.queryTokensInBytesRef = new ArrayList<>();
            for(String token: queryTokens) {
                BytesRef byteRef = new BytesRef(token.toLowerCase().getBytes());
                this.queryTokensInBytesRef.add(byteRef);
            }

            this.attributeList = dataReaderPredicate.getAttributeList();
            this.schema = dataReaderPredicate.getDataStore().getSchema();
            this.spanSchema = Utils.createSpanSchema(schema);

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
            Document document = luceneIndexSearcher.doc(scoreDocs[cursor].doc);
            List<Span> spanList = new ArrayList<>();
            List<IField> fields = new ArrayList<IField>();

            for (Attribute  attr : schema.getAttributes()) {
                FieldType fieldType = attr.getFieldType();
                String fieldValue = document.get(attr.getFieldName());
                fields.add(Utils.getField(fieldType, fieldValue));
            }

            // If the span Information is not requested,
            // just return the dataTuple without span information.

            if(!dataReaderPredicate.getIsSpanInformationAdded()){
                cursor++;
                DataTuple dataTuple = new DataTuple(schema, fields.toArray(new IField[fields.size()]));
                return  dataTuple;
            }

            // Create span information.

            for(Attribute attr: attributeList){

                String fieldName  = attr.getFieldName();
                // Get the term vector for the current field.
                Terms vector = luceneIndexReader.getTermVector(scoreDocs[cursor].doc,fieldName);

                if (vector != null) {
                    TermsEnum vectorEnum = vector.iterator();
                    int queryTokenIndex = 0;
                    // Search for all the query tokens in the term vector one by one.
                    for(BytesRef term: queryTokensInBytesRef){

                        //If Term is found, calculate the position info and add to the Spans
                        if(vectorEnum.seekExact(term)){
                            PostingsEnum postings = vectorEnum.postings(null, PostingsEnum.POSITIONS);

                            while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                                int freq = postings.freq();
                                // Create a new span for every occurrence.
                                while (freq-- > 0) {
                                    int tokenOffset = postings.nextPosition();
                                    int start = postings.startOffset();
                                    int end = start+term.length;
                                    String key = queryTokens.get(queryTokenIndex);
                                    String value = document.get(fieldName).substring(start,end);
                                    Span span = new Span(fieldName, start, end, key, value, tokenOffset);
                                    spanList.add(span);
                                }

                            }

                        }

                        queryTokenIndex++;
                    }


                }



            }

            cursor++;



            ITuple dataTuple  = Utils.getSpanTuple(fields, spanList, spanSchema);
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
        if(luceneIndexReader != null){
            try {
                luceneIndexReader.close();
                luceneIndexReader = null;
            } catch (IOException e) {
                e.printStackTrace();
                throw new DataFlowException(e.getMessage(), e);
            }
        }
    }
    
}
