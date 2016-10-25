package edu.uci.ics.textdb.dataflow.comparablematcher;

import edu.uci.ics.textdb.api.common.*;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.AbstractSingleInputOperator;

/**
 * ComparableMatcher is matcher for comparison query on any field which deals with Comparable.
 *
 * @author Adrian Seungjin Lee
 */
public class ComparableMatcher<T extends Comparable> extends AbstractSingleInputOperator {
    private ComparablePredicate<T> predicate;

    private Schema inputSchema;

    public ComparableMatcher(ComparablePredicate<T> predicate) {
        this.predicate = predicate;
    }

    @Override
    protected void setUp() throws DataFlowException {
        inputSchema = inputOperator.getOutputSchema();
        outputSchema = inputSchema;
    }

    @Override
    protected ITuple computeNextMatchingTuple() throws Exception {
        ITuple inputTuple = null;
        ITuple resultTuple = null;

        while ((inputTuple = inputOperator.getNextTuple()) != null) {
            Attribute attribute = predicate.getAttribute();
            DataConstants.NumberMatchingType operatorType = predicate.getMatchingType();

            FieldType fieldType = attribute.getFieldType();
            String fieldName = attribute.getFieldName();

            T value;
            T threshold;
            try {
                value = (T) inputTuple.getField(fieldName).getValue();
                threshold = (T) predicate.getThreshold();
            } catch (ClassCastException e) {
                continue;
            }

            if (compareValues(value, threshold, operatorType)) {
                resultTuple = inputTuple;
            }

            if (resultTuple != null) {
                break;
            }
        }
        return resultTuple;
    }

    private boolean compareValues(T value, T threshold, DataConstants.NumberMatchingType operatorType) {
        int compareResult = value.compareTo(threshold);
        switch (operatorType) {
            case EQUAL_TO:
                if (compareResult == 0) {
                    return true;
                }
                break;
            case GREATER_THAN:
                if (compareResult == 1) {
                    return true;
                }
                break;
            case GREATER_THAN_OR_EQUAL_TO:
                if (compareResult == 0 || compareResult == 1) {
                    return true;
                }
                break;
            case LESS_THAN:
                if (compareResult == -1) {
                    return true;
                }
                break;
            case LESS_THAN_OR_EQUAL_TO:
                if (compareResult == 0 || compareResult == -1) {
                    return true;
                }
                break;
            case NOT_EQUAL_TO:
                if (compareResult != 0) {
                    return true;
                }
                break;
        }
        return false;
    }

    @Override
    protected void cleanUp() throws DataFlowException {
    }

}
