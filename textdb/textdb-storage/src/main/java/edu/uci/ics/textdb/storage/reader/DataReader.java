package edu.uci.ics.textdb.storage.reader;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
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
	private boolean termVecAdded = true;
	
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
		ArrayList<IField> docFields = documentToFields(document);
		
		if (this.termVecAdded) {
			ArrayList<Span> payloadSpanList = buildPayloadFromTermVector(docFields, docID);
			ListField<Span> payloadField = new ListField<Span>(payloadSpanList);
			docFields.add(payloadField);
		}
		
		DataTuple resultTuple = new DataTuple(outputSchema, docFields.stream().toArray(IField[]::new));
		return resultTuple;
	}
	
	
	private ArrayList<IField> documentToFields(Document document) throws ParseException {
		ArrayList<IField> fields = new ArrayList<>();
        for (Attribute attr : this.inputSchema.getAttributes()) {
            FieldType fieldType = attr.getFieldType();
            String fieldValue = document.get(attr.getFieldName());
            fields.add(Utils.getField(fieldType, fieldValue));
        }
        return fields;
	}
	
	
	private ArrayList<Span> buildPayloadFromTermVector(List<IField> fields, int docID) throws IOException {
		ArrayList<Span> payloadSpanList = new ArrayList<>();

		for (Attribute attr : this.inputSchema.getAttributes()) {
			String fieldName = attr.getFieldName();
			FieldType fieldType = attr.getFieldType();
			
			if (fieldType != FieldType.TEXT) {
				continue;
			}
			
			String fieldValue = fields.get(inputSchema.getIndex(fieldName)).getValue().toString();
			
			Terms termVector = this.indexReader.getTermVector(docID, fieldName);			
			if (termVector == null) {
				return null;
			}
			
			TermsEnum termsEnum = termVector.iterator();
			PostingsEnum termPostings = null;
			
			while ((termsEnum.next()) != null) {
				termPostings = termsEnum.postings(termPostings, PostingsEnum.ALL);
				if (termPostings.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
					continue;
				}
				for (int i = 0; i < termPostings.freq(); i++) {
		        	int position = termPostings.nextPosition(); // nextPosition needs to be called first
		        	
		        	int start = termPostings.startOffset();
		        	int end = termPostings.endOffset();
		        	String termStr = termsEnum.term().utf8ToString();
		        	String actualStr = fieldValue.substring(start, end);

					Span span = new Span(fieldName, start, end, termStr, actualStr, position);
					payloadSpanList.add(span);
				}
			}
		}
		
		return payloadSpanList;
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
