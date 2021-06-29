package edu.uci.ics.texera.workflow.operators.visualization;

import edu.uci.ics.texera.workflow.common.IncrementalOutputMode;
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;

/**
 * Base class for visualization operators. Visualization Operators should precede ViewResult Operator.
 * Author: Mingji Han, Xiaozhen Liu
 */
public abstract class VisualizationOperator extends OperatorDescriptor {

    public abstract String chartType();

    // visualization operators have SET_SNAPSHOT incremental output mode by default
    // an operator can override this option if it wishes to output in other incremental output mode
    public IncrementalOutputMode outputMode() {
        return IncrementalOutputMode.SET_SNAPSHOT;
    }

}
