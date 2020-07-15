package edu.uci.ics.texera.dataflow.sink;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.dataflow.ISink;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.List;

public abstract class AbstractTupleSink implements ISink {
    protected IOperator inputOperator;
    protected int cursor = CLOSED;
    protected Schema outputSchema;

    public void setInputOperator(IOperator inputOperator) {
        if (cursor != CLOSED) {
            throw new TexeraException(ErrorMessages.INPUT_OPERATOR_CHANGED_AFTER_OPEN);
        }
        this.inputOperator = inputOperator;
    }

    public abstract List<Tuple> collectAllTuples();

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return ;
        }

        if (inputOperator != null)
            inputOperator.close();

        cursor = CLOSED;
    }

    @Override
    public Schema transformToOutputSchema(Schema... inputSchema) {
        throw new TexeraException(ErrorMessages.INVALID_OUTPUT_SCHEMA_FOR_SINK);
    }

}
