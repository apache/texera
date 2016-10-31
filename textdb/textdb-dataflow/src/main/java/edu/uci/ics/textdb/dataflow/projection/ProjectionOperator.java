package edu.uci.ics.textdb.dataflow.projection;

import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.common.Schema;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.common.field.DataTuple;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;

public class ProjectionOperator extends AbstractSingleInputOperator {
    
    ProjectionPredicate predicate;
    
    Schema inputSchema;
    
    public ProjectionOperator(ProjectionPredicate predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        List<Attribute> outputAttributes = 
                inputSchema.getAttributes()
                .stream()
                .filter(attr -> predicate.getProjectionFields().contains(attr.getFieldName().toLowerCase()))
                .collect(Collectors.toList());
        
        if (outputAttributes.size() != predicate.getProjectionFields().size()) {
            throw new DataFlowException("input schema doesn't contain one of the attributes to be projected");
        }
        outputSchema = new Schema(outputAttributes.stream().toArray(Attribute[]::new));
    }

    @Override
    protected ITuple computeNextMatchingTuple() throws Exception {
        ITuple inputTuple = inputOperator.getNextTuple();
        if (inputTuple == null) {
            return null;
        }

        return processOneInputTuple(inputTuple);
    }

    @Override
    public ITuple processOneInputTuple(ITuple tuple) throws DataFlowException {
        IField[] outputFields =
                outputSchema.getAttributes()
                        .stream()
                        .map(attr -> tuple.getField(attr.getFieldName()))
                        .toArray(IField[]::new);

        return new DataTuple(outputSchema, outputFields);
    }

    @Override
    protected void cleanUp() throws DataFlowException {        
    }

    
}
