package edu.uci.ics.textdb.dataflow.sink;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.StorageException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.common.field.ListField;
import edu.uci.ics.textdb.storage.DataStore;
import edu.uci.ics.textdb.storage.catalog.CatalogManager;
import edu.uci.ics.textdb.storage.writer.DataWriter;

/**
 * IndexSink is a sink that writes tuples into an index.
 * 
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {

    private String collectionName;
    private String indexDirectory;
    
    private Analyzer luceneAnalyzer;
    private String luceneAnalyzerStr;
    
    private DataWriter dataWriter;
    private Schema outputSchema;
    
    private boolean isAppend = false;
    

    public IndexSink(String collectionName, String indexDirectory, Analyzer luceneAnalyzer, boolean isAppend) {
        this.collectionName = collectionName;
        this.indexDirectory = indexDirectory;
        this.luceneAnalyzer = luceneAnalyzer;
        this.isAppend = isAppend;
    }

    public void open() throws Exception {
        super.open();
        
        this.outputSchema = processOutputSchema(this.getInputOperator().getOutputSchema());
        
        DataStore dataStore = new DataStore(indexDirectory, outputSchema);
        this.dataWriter = new DataWriter(dataStore, luceneAnalyzer);
        
        if (! isAppend) {
            this.dataWriter.clearData();
            
            if (luceneAnalyzer instanceof StandardAnalyzer) {
                this.luceneAnalyzerStr = DataConstants.STANDARD_LUCENE_ANALYZER;
            } else {
                this.luceneAnalyzerStr = DataConstants.TRIGRAM_LUCENE_ANALYZER;
            }
            CatalogManager.putCollectionSchema(
                    collectionName, indexDirectory, 
                    outputSchema, luceneAnalyzerStr);
        }
        
        this.dataWriter.open();
    }

    protected void processOneTuple(ITuple nextTuple) throws StorageException {
        dataWriter.writeTuple(processOutputTuple(nextTuple));
    }

    public void close() throws Exception {
        if (this.dataWriter != null) {
            this.dataWriter.close();
        }
        super.close();
    }
    
    /*
     * Return the schema to be written into the index.
     * This function removes the attributes with FieldType LIST, 
     * because currently DataWriter doesn't support LIST type.
     */
    private Schema processOutputSchema(Schema inputSchema) {
        return new Schema(
                inputSchema.getAttributes().stream()
                .filter(attr -> attr.getFieldType() != FieldType.LIST)
                .toArray(Attribute[]::new));
    }
    
    /*
     * Return the tuple to be written into the index.
     * This function removes the fields with FieldType LIST, 
     * because currently DataWriter doesn't support LIST type.
     */
    private ITuple processOutputTuple(ITuple inputTuple) {
        List<IField> newFields = inputTuple.getFields().stream()
                .filter(field -> !(field instanceof ListField)).collect(Collectors.toList());
        return new DataTuple(outputSchema, newFields.stream().toArray(IField[]::new));
    }

}
