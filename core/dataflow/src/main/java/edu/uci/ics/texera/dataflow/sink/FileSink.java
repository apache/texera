package edu.uci.ics.texera.dataflow.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;

/**
 * Created by chenli on 5/11/16.
 *
 * This class serializes each tuple from the subtree to a given file.
 */
public class FileSink extends AbstractSink {
    
    @FunctionalInterface
    public static interface TupleToString {
        String convertToString(Tuple tuple);
    }

    private PrintWriter printWriter;
    private final File file;   
    private TupleToString toStringFunction = (tuple -> tuple.toString());
    
    public FileSink(File file) {
        this.file = file;
    }
    
    public void setToStringFunction(TupleToString toStringFunction) {
        this.toStringFunction = toStringFunction;
    }

    @Override
    public void open() throws TexeraException {
        super.open();
        try {
            this.printWriter = new PrintWriter(file);
        } catch (FileNotFoundException e) {
            throw new TexeraException("Failed to open file sink", e);
        }
    }

    @Override
    public void close() throws TexeraException {
        if (this.printWriter != null) {
            this.printWriter.close();
        }
        super.close();
    }

    @Override
    protected void processOneTuple(Tuple nextTuple) {
        printWriter.write(toStringFunction.convertToString(nextTuple));
    }
    
    public File getFile() {
        return this.file;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) throws DataflowException {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }
}
