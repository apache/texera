package edu.uci.ics.texera.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.constants.SchemaConstants;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.ListField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.span.Span;
import edu.uci.ics.texera.api.tuple.*;
import edu.uci.ics.texera.storage.utils.StorageUtils;

/**
 * DataReader is the layer where Texera handles upper-level operators' read operations
 *   and performs corresponding operations to Lucene.
 *   
 * DataReader can get tuples from the Lucene index folder by a lucene query,
 *   and return the tuples in an iterative way through "getNextTuple()"
 * 
 * DataReader currently has the option to append a "payload" field to a tuple, the "payload" field is a list of spans. 
 * Each span contains the start, end, and token offset position of a token in the original document.
 * The "payload" contains spans for EVERY token in tuple.
 * 
 * The purpose of the "payload" field is to make subsequent keyword match, fuzzy token match, and dictionary match faster,
 * because they don't need to tokenize the tuple every time.
 *   
 * 
 * DataReader for a specific table is only accessible from RelationManager.
 * 
 * 
 * @author Zuozhi Wang
 *
 */
public class DataReader implements IOperator {

    private DataStore dataStore;
    private Query query;
    
    private Schema inputSchema;
    private Schema outputSchema;

    private IndexReader luceneIndexReader;
    private IndexSearcher luceneIndexSearcher;
    private ScoreDoc[] scoreDocs;

    private int cursor = CLOSED;

    private boolean payloadAdded;

    /*
     * The package-only level constructor is only accessible inside the storage package.
     * Only the RelationManager is allowed to constructor a DataWriter object, 
     *  while upper-level operators can't.
     */
    DataReader(DataStore dataStore, Query query) {
        this(dataStore, query, false);
    }
    
    DataReader(DataStore dataStore, Query query, boolean payloadAdded) {
        this.dataStore = dataStore;
        this.query = query;
        this.payloadAdded = payloadAdded;
    }

    @Override
    public void open() throws StorageException {
        if (cursor != CLOSED) {
            return;
        }
        try {
            Directory indexDirectory = FSDirectory.open(this.dataStore.getDataDirectory());
            luceneIndexReader = DirectoryReader.open(indexDirectory);
            luceneIndexSearcher = new IndexSearcher(luceneIndexReader);

            TopDocs topDocs = luceneIndexSearcher.search(query, Integer.MAX_VALUE);
            scoreDocs = topDocs.scoreDocs;

            inputSchema = this.dataStore.getSchema();
            if (payloadAdded) {
                outputSchema = new Schema.Builder(inputSchema).add(SchemaConstants.PAYLOAD_ATTRIBUTE).build();
            } else {
                outputSchema = inputSchema;
            }

        } catch (IOException e) {
            throw new StorageException(e.getMessage(), e);
        }

        cursor = OPENED;
    }

    @Override
    public Tuple getNextTuple() throws StorageException {
        if (cursor == CLOSED) {
            throw new StorageException(ErrorMessages.OPERATOR_NOT_OPENED);
        }

        Tuple resultTuple;
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

    private Tuple constructTuple(int docID) throws IOException, ParseException {
        Document luceneDocument = luceneIndexSearcher.doc(docID);
        ArrayList<IField> docFields = documentToFields(luceneDocument);

        if (payloadAdded) {
            ArrayList<Span> payloadSpanList = buildPayloadFromTermVector(docFields, docID);
            ListField<Span> payloadField = new ListField<Span>(payloadSpanList);
            docFields.add(payloadField);
        }

        Tuple resultTuple = new Tuple(outputSchema, docFields.stream().toArray(IField[]::new));
        return resultTuple;
    }

    private ArrayList<IField> documentToFields(Document luceneDocument) throws ParseException {
        ArrayList<IField> fields = new ArrayList<>();
        for (Attribute attr : inputSchema.getAttributes()) {
            AttributeType attributeType = attr.getType();
            String fieldValue = luceneDocument.get(attr.getName());
            fields.add(StorageUtils.getField(attributeType, fieldValue));
        }
        return fields;
    }

    private ArrayList<Span> buildPayloadFromTermVector(List<IField> fields, int docID) throws IOException {
        ArrayList<Span> payloadSpanList = new ArrayList<>();

        for (Attribute attr : inputSchema.getAttributes()) {
            String attributeName = attr.getName();
            AttributeType attributeType = attr.getType();

            // We only store positional information for TEXT fields into
            // payload.
            if (attributeType != AttributeType.TEXT) {
                continue;
            }

            String fieldValue = fields.get(inputSchema.getIndex(attributeName)).getValue().toString();

            Terms termVector = luceneIndexReader.getTermVector(docID, attributeName);
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

                    Span span = new Span(attributeName, charStart, charEnd, analyzedTermStr, originalTermStr,
                            tokenPosition);
                    payloadSpanList.add(span);
                }
            }
        }

        return payloadSpanList;
    }
    
    public boolean isPayloadAdded() {
        return this.payloadAdded;
    }
    
    public void setPayloadAdded(boolean payloadAdded) {
        this.payloadAdded = payloadAdded;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }
    
    public static boolean checkIndexExistence(Path directory) {
        try {
            return DirectoryReader.indexExists(
                    FSDirectory.open(directory));
        } catch (IOException e) {
            return false;
        }
    }
    
    public IndexReader getLuceneIndexReader() {
        return this.luceneIndexReader;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_FUNCTION_CALL);
    }
}