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
import edu.uci.ics.textdb.common.exception.ErrorMessages;
import edu.uci.ics.textdb.common.exception.StorageException;
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

    private IndexReader luceneIndexReader;
    private IndexSearcher luceneIndexSearcher;
    private ScoreDoc[] scoreDocs;

    private int cursor = CLOSED;

    private int limit;
    private int offset;
    private boolean payloadAdded;

    public DataReader(DataReaderPredicate dataReaderPredicate) {
        predicate = dataReaderPredicate;
        payloadAdded = dataReaderPredicate.isPayloadAdded();
    }

    @Override
    public void open() throws StorageException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            String indexDirectoryStr = predicate.getDataStore().getDataDirectory();
            Directory indexDirectory = FSDirectory.open(Paths.get(indexDirectoryStr));
            luceneIndexReader = DirectoryReader.open(indexDirectory);
            luceneIndexSearcher = new IndexSearcher(luceneIndexReader);

            TopDocs topDocs = luceneIndexSearcher.search(predicate.getLuceneQuery(), Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;

            inputSchema = predicate.getDataStore().getSchema();
            if (payloadAdded) {
                outputSchema = Utils.addAttributeToSchema(inputSchema, SchemaConstants.PAYLOAD_ATTRIBUTE);
            } else {
                outputSchema = inputSchema;
            }

        } catch (IOException e) {
            throw new StorageException(e.getMessage(), e);
        }

        cursor = OPENED;
    }

    @Override
    public ITuple getNextTuple() throws StorageException {
        if (cursor == CLOSED) {
            throw new StorageException(ErrorMessages.OPERATOR_NOT_OPENED);
        }

        ITuple resultTuple;
        try {
            if (cursor >= scoreDocs.length) {
                return null;
            }
            int docID = scoreDocs[cursor].doc;
            resultTuple = constructTuple(docID);

        } catch (IOException | ParseException e) {
            throw new StorageException(e.getMessage(), e);
        }

        cursor++;
        return resultTuple;
    }

    @Override
    public void close() throws StorageException {
        cursor = CLOSED;
        if (luceneIndexReader != null) {
            try {
                luceneIndexReader.close();
                luceneIndexReader = null;
            } catch (IOException e) {
                throw new StorageException(e.getMessage(), e);
            }
        }
    }

    private ITuple constructTuple(int docID) throws IOException, ParseException {
        Document luceneDocument = luceneIndexSearcher.doc(docID);
        ArrayList<IField> docFields = documentToFields(luceneDocument);

        if (payloadAdded) {
            ArrayList<Span> payloadSpanList = buildPayloadFromTermVector(docFields, docID);
            ListField<Span> payloadField = new ListField<Span>(payloadSpanList);
            docFields.add(payloadField);
        }

        DataTuple resultTuple = new DataTuple(outputSchema, docFields.stream().toArray(IField[]::new));
        return resultTuple;
    }

    private ArrayList<IField> documentToFields(Document luceneDocument) throws ParseException {
        ArrayList<IField> fields = new ArrayList<>();
        for (Attribute attr : inputSchema.getAttributes()) {
            FieldType fieldType = attr.getFieldType();
            String fieldValue = luceneDocument.get(attr.getFieldName());
            fields.add(Utils.getField(fieldType, fieldValue));
        }
        return fields;
    }

    private ArrayList<Span> buildPayloadFromTermVector(List<IField> fields, int docID) throws IOException {
        ArrayList<Span> payloadSpanList = new ArrayList<>();

        for (Attribute attr : inputSchema.getAttributes()) {
            String fieldName = attr.getFieldName();
            FieldType fieldType = attr.getFieldType();

            // We only store positional information for TEXT fields into
            // payload.
            if (fieldType != FieldType.TEXT) {
                continue;
            }

            String fieldValue = fields.get(inputSchema.getIndex(fieldName)).getValue().toString();

            Terms termVector = luceneIndexReader.getTermVector(docID, fieldName);
            if (termVector == null) {
                continue;
            }

            TermsEnum termsEnum = termVector.iterator();
            PostingsEnum termPostings = null;
            // go through document terms
            while ((termsEnum.next()) != null) {
                termPostings = termsEnum.postings(termPostings, PostingsEnum.ALL);
                if (termPostings.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
                    continue;
                }
                // for each term, go through its postings
                for (int i = 0; i < termPostings.freq(); i++) {
                    int tokenPosition = termPostings.nextPosition(); // nextPosition needs to be called first
                    int charStart = termPostings.startOffset();
                    int charEnd = termPostings.endOffset();
                    String analyzedTermStr = termsEnum.term().utf8ToString();
                    String originalTermStr = fieldValue.substring(charStart, charEnd);

                    Span span = new Span(fieldName, charStart, charEnd, analyzedTermStr, originalTermStr,
                            tokenPosition);
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

    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public static boolean checkIndexExistence(String directory) {
        try {
            return DirectoryReader.indexExists(
                    FSDirectory.open(Paths.get(directory)));
        } catch (IOException e) {
            return false;
        }
    }
    
}