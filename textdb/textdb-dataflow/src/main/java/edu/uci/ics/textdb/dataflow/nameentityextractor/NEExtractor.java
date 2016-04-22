package edu.uci.ics.textdb.dataflow.nameentityextractor;

import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.DataFlowException;


/**
 * Created by Sam on 16/4/21.
 */
public class NEExtractor implements IOperator {


    private IOperator operator;


    public NEExtractor(IOperator operator) {
        this.operator = operator;
    }


    /**
     * @about Opens name entities matcher.
     *
     *  Create
     *
     */
    @Override
    public void open() throws Exception {
        try {
            operator.open();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    /**
     * @about Gets next Name Entities tuple. Returns a new span tuple including the
     *        span results.
     *
     * @overview  First get next tuple form the lower level operator, then process it
     * with Stanford NLP package  and return the next Name Entities Tuple
     *
     */
    @Override
    public ITuple getNextTuple() throws Exception {
        return null;
    }

    /**
     * @about Modifies schema, fields and creates a new span tuple for return
     */
    private ITuple getSpanTuple() {
        return null;
    }

    /**
     * @about Closes the operator
     */
    @Override
    public void close() throws DataFlowException {
        try {
            operator.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }
}
