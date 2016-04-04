package edu.uci.ics.textdb.api.dataflow;

import edu.uci.ics.textdb.api.common.ITuple;

/**
 * Created by chenli on 3/25/16.
 */
public interface IOperator {

    void open();

    ITuple getNextTuple();

    void close();
}
