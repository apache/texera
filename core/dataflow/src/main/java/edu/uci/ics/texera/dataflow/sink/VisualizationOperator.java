package edu.uci.ics.texera.dataflow.sink;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.texera.api.constants.ErrorMessages;
import edu.uci.ics.texera.api.exception.TexeraException;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.Tuple;


/**
 * Base class for visualization operators
 * Author: Mingji Han
 */
public abstract class VisualizationOperator extends AbstractTupleSink {


    protected List<Tuple> result = new ArrayList<>();
    protected final String type;

    public VisualizationOperator(String type) {
        this.type = type;
    }



    @Override
    public Tuple getNextTuple() throws TexeraException {
        return inputOperator.getNextTuple();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }


    public List<Tuple> collectAllTuples() {
        processTuples();
        return result;
    }



    public String getChartType() {
        return type;
    }

}
