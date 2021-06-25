package edu.uci.ics.texera.dataflow.plangen;

import edu.uci.ics.texera.api.exception.TexeraException;

/**
 * This class provides a set of helper functions that are commonly used in plan generation.
 * @author Zuozhi Wang
 *
 */
public class PlanGenUtils {

    public static void planGenAssert(boolean assertBoolean, String errorMessage) {
        if (! assertBoolean) {
            throw new TexeraException(errorMessage);
        }
    }

}
