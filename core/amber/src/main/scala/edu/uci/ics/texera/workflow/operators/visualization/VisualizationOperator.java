package edu.uci.ics.texera.workflow.operators.visualization;

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor;

/**
 * Base class for visualization operators. Visualization Operators should precede ViewResult Operator.
 * Author: Mingji Han, Xiaozhen Liu
 */
public abstract class VisualizationOperator extends OperatorDescriptor {
    public abstract String chartType();
}
