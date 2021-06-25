package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.StorageException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.storage.DataWriter;
import edu.uci.ics.texera.storage.RelationManager;

/**
 * IndexSink is a sink that writes tuples into an index.
 * 
 * @author zuozhi
 */
public class IndexSink extends AbstractSink {

    private DataWriter dataWriter;
    private boolean isAppend = false;

    public IndexSink(String tableName, boolean isAppend) throws DataflowException {
        try {
            RelationManager relationManager = RelationManager.getInstance();
            this.dataWriter = relationManager.getTableDataWriter(tableName);
            this.isAppend = isAppend;
        } catch (StorageException e) {
            throw new DataflowException(e);
        }

    }

    public void open() throws TexeraException {
        super.open();
        this.dataWriter.open();
        if (! this.isAppend) {
            this.dataWriter.clearData();
        }
    }

    protected void processOneTuple(Tuple nextTuple) throws TexeraException {
        dataWriter.insertTuple(nextTuple);
    }

    public void close() throws TexeraException {
        if (this.dataWriter != null) {
            this.dataWriter.close();
        }
        super.close();
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }

}
