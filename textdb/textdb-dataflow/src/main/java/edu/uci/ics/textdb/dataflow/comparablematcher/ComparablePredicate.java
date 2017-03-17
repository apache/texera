package edu.uci.ics.textdb.dataflow.comparablematcher;

import edu.uci.ics.textdb.api.constants.DataConstants.NumberMatchingType;
import edu.uci.ics.textdb.api.dataflow.IPredicate;
import edu.uci.ics.textdb.api.exception.DataFlowException;
import edu.uci.ics.textdb.api.schema.Attribute;

/**
 *
 * @author Adrian Seungjin Lee
 *
 */
public class ComparablePredicate<T extends Comparable> implements IPredicate {

    private Attribute attribute;
    private T threshold;
    private NumberMatchingType matchingType;

    public ComparablePredicate(T threshold, Attribute attribute,
                               NumberMatchingType matchingType) throws DataFlowException {
        try {
            this.threshold = threshold;
            this.attribute = attribute;
            this.matchingType = matchingType;
        } catch (Exception e) {
            e.printStackTrace();
            throw new DataFlowException(e.getMessage(), e);
        }
    }

    public NumberMatchingType getMatchingType() {
        return matchingType;
    }

    public Attribute getAttribute() {
        return attribute;
    }

    public T getThreshold() {
        return threshold;
    }

}
