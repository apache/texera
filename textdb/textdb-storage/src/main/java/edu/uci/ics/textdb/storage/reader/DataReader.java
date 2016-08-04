package edu.uci.ics.textdb.storage.reader;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.constants.SchemaConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.common.field.Span;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.storage.DataReaderPredicate;

/**
 * 
 * @author Zuozhi Wang
 *
 */

public class DataReader implements IDataReader {
	
	private DataReaderPredicate predicate;
	private Schema inputSchema;
	private Schema outputSchema;

	private IndexReader indexReader;
	private IndexSearcher indexSearcher;
	private ScoreDoc[] scoreDocs;
	
	private int cursor = CLOSED;

	private int limit;
	private int offset;
	private boolean termVecAdded;
	
	public DataReader(DataReaderPredicate dataReaderPredicate) {
		this.predicate = dataReaderPredicate;
	}
	
	
	@Override
	public void open() throws DataFlowException {
		try {
			String indexDirectoryStr = predicate.getDataStore().getDataDirectory();
			Directory indexDirectory = FSDirectory.open(Paths.get(indexDirectoryStr));
			this.indexReader = DirectoryReader.open(indexDirectory);
			
			this.indexSearcher = new IndexSearcher(indexReader);
			TopDocs topDocs = indexSearcher.search(predicate.getLuceneQuery(), Integer.MAX_VALUE);
			this.scoreDocs = topDocs.scoreDocs;
			
			this.inputSchema = predicate.getDataStore().getSchema();
			if (this.termVecAdded) {
				this.outputSchema = Utils.appendAttributeToSchema(this.inputSchema, SchemaConstants.TERM_VECTOR_ATTRIBUTE);
			} else {
				this.outputSchema = predicate.getDataStore().getSchema();
			}
				
		} catch (IOException e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
		
		cursor = OPENED;
	}
	

	@Override
	public ITuple getNextTuple() throws DataFlowException {
		if (this.cursor == CLOSED) {
			throw new DataFlowException(ErrorMessages.OPERATOR_NOT_OPENED);
		}
		
		ITuple resultTuple;
        try {
    		if (this.cursor >= this.scoreDocs.length) {
    			return null;
    		}
    		int docID = scoreDocs[cursor].doc;
    		resultTuple = constructTuple(docID);

			
		} catch (IOException | ParseException e) {
			e.printStackTrace();
			throw new DataFlowException(e.getMessage(), e);
		}
        
        cursor++;	
		return resultTuple;
	}

	
	@Override
	public void close() throws DataFlowException {
		cursor = CLOSED;
		if (indexReader != null) {
			try {
				indexReader.close();
				indexReader = null;
			} catch (IOException e) {
				throw new DataFlowException(e.getMessage(), e);
			}
		}
	}
	
	
	private ITuple constructTuple(int docID) throws IOException, ParseException {
		Document document = this.indexSearcher.doc(docID);
		ITuple resultTuple = documentToTuple(document);
		
		if (this.termVecAdded) {
			ArrayList<Span> termVecList = new ArrayList<>();
			for (Attribute attr : this.inputSchema.getAttributes()) {
				String fieldName = attr.getFieldName();
				Terms termVector = indexReader.getTermVector(docID, fieldName);
				termVecList.addAll(termVecToSpanList(termVector, fieldName));
			}
			
			ArrayList<IField> fields = new ArrayList<>(resultTuple.getFields());
			fields.add(new ListField<Span>(termVecList));
			
			resultTuple = new DataTuple(outputSchema, fields.stream().toArray(IField[]::new));
		}
		return resultTuple;
	}
	
	
	private ITuple documentToTuple(Document document) throws ParseException {
		ArrayList<IField> fields = new ArrayList<>();
        for (Attribute attr : this.inputSchema.getAttributes()) {
            FieldType fieldType = attr.getFieldType();
            String fieldValue = document.get(attr.getFieldName());
            fields.add(Utils.getField(fieldType, fieldValue));
        }
        DataTuple resultTuple = new DataTuple(this.inputSchema, fields.stream().toArray(IField[]::new));  
        return resultTuple;
	}
	
	
	private ArrayList<Span> termVecToSpanList(Terms termVector, String fieldName) throws IOException {
		if (termVector == null) {
			return null;
		}
		
		ArrayList<Span> termVecList = new ArrayList<>();
		TermsEnum termsEnum = termVector.iterator();
		PostingsEnum termPostings = null;
		
		while ((termsEnum.next()) != null) {
			termPostings = termsEnum.postings(termPostings, PostingsEnum.POSITIONS);
			for (int i = 0; i < termPostings.freq(); i++) {
				Span span = new Span(
						fieldName,	// field
						termPostings.startOffset(), 	// start
						termPostings.endOffset(), 		// end
						termPostings.getPayload().utf8ToString(), // key
						termPostings.getPayload().utf8ToString(), // value
						termPostings.nextPosition());	// token offset
				termVecList.add(span);
			}
		}
		
		return termVecList;
	}
	
	
	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public boolean isTermVecAdded() {
		return termVecAdded;
	}

	public void setTermVecAdded(boolean termVecAdded) {
		this.termVecAdded = termVecAdded;
	}
	
	public Schema getOutputSchema() {
		return outputSchema;
	}
}
