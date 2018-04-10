package edu.uci.ics.texera.dataflow.projection;

import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.DataflowException;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;
import edu.uci.ics.texera.dataflow.common.AbstractSingleInputOperator;

public class ProjectionOperator extends AbstractSingleInputOperator {
    
    ProjectionPredicate predicate;
    
    Schema inputSchema;
    
    public ProjectionOperator(ProjectionPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws TexeraException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = transformToOutputSchema(inputSchema);
    }

    @Override
    protected Tuple computeNextMatchingTuple() throws TexeraException {
        Tuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }

        return processOneInputTuple(inputTuple);
    }

    @Override
    public Tuple processOneInputTuple(Tuple inputTuple) throws TexeraException {
        IField[] outputFields =
                outputSchema.getAttributes()
                        .stream()
                        .map(attr -> inputTuple.getField(attr.getName()))
                        .toArray(IField[]::new);

        return new Tuple(outputSchema, outputFields);
    }

    @Override
    protected void cleanUp() throws DataflowException {        
    }

    public ProjectionPredicate getPredicate() {
        return predicate;
    }

    public Schema transformToOutputSchema(Schema... inputSchema) {
        if (inputSchema.length != 1)
            throw new TexeraException(String.format(ErrorMessages.NUMBER_OF_ARGUMENTS_DOES_NOT_MATCH, 1, inputSchema.length));

        List<Attribute> outputAttributes =
                inputSchema[0].getAttributes()
                        .stream()
                        .filter(attr -> predicate.getProjectionFields().contains(attr.getName().toLowerCase()))
                        .collect(Collectors.toList());

        if (outputAttributes.size() != predicate.getProjectionFields().size()) {
            throw new DataflowException("input schema doesn't contain one of the attributes to be projected");
        }
        return new Schema(outputAttributes.stream().toArray(Attribute[]::new));
    }
}
