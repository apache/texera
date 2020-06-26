package edu.uci.ics.texera.dataflow.nlp.preprocessing;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.dataflow.IOperator;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;

public class ToLowerCaseOperator implements IOperator {
    private IOperator inputOperator;
    private Schema outputSchema;
    private ToLowerCasePredicate predicate;
    private int cursor = CLOSED;
    private AttributeType outputAttributeType;

    public ToLowerCaseOperator(ToLowerCasePredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    public void open() throws TexeraException {
        if (cursor != CLOSED) {
           return ;
        }

        if (inputOperator == null) {
            throw new DataflowException(ErrorMessages.INPUT_OPERATOR_NOT_SPECIFIED);
        }

        inputOperator.open();
        // We only wants to convert the upper case to lower case so schema should not be change.
        Schema inputSchema = inputOperator.getOutputSchema();

        // Check the attribute

        String attribute = predicate.getInputAttributeName();
        AttributeType type = inputSchema.getAttribute(attribute).getType();
        boolean isValid = type.equals(AttributeType.TEXT) || type.equals(AttributeType.STRING);
        if (!isValid) {
            throw new DataflowException(String.format(
                "input attribute %s must have type String or Text, its actual type is %s",
                predicate.getInputAttributeName(),
                type));
        }

        outputSchema = transformToOutputSchema(inputSchema);
        outputAttributeType = type;
        cursor = OPENED;

    }

    @Override
    public Tuple getNextTuple() throws TexeraException {

        Tuple currentTuple = inputOperator.getNextTuple();
        if (currentTuple == null) {
            return null;
        }

        List<IField> outputFields = new ArrayList<>(currentTuple.getFields());

        // Get the input attribute value in the tuple
        String inputAttributeValue = currentTuple.getField(predicate.getInputAttributeName()).getValue().toString();
        // Convert it into lower case
        if (outputAttributeType.equals(AttributeType.STRING)) {
            outputFields.add(new StringField(inputAttributeValue.toLowerCase()));
        }
        else {
            outputFields.add(new TextField(inputAttributeValue.toLowerCase()));
        }
        return new Tuple(outputSchema, outputFields);
    }

    @Override
    public void close() throws TexeraException {
        if (cursor == CLOSED) {
            return ;
        }
        if (inputOperator != null) {
            inputOperator.close();
        }
        cursor = CLOSED;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        String inputAttributeName = predicate.getInputAttributeName();
        String resultAttributeName = predicate.getResultAttributeName();
        Schema.checkAttributeExists(inputSchema[0], inputAttributeName);
        Schema.checkAttributeNotExists(inputSchema[0], resultAttributeName);

        AttributeType attributeType = inputSchema[0].getAttribute(predicate.getInputAttributeName()).getType();

        return new Schema.Builder().add(inputSchema[0]).add(resultAttributeName, attributeType).build();
    }

    public void setInputOperator(IOperator inputOperator) throws DataflowException {
        if (cursor != CLOSED) {
            throw new DataflowException("Cannot link this operator to other operator after the operator is opened");
        }
        this.inputOperator = inputOperator;
    }
}
