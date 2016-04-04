package edu.uci.ics.textdb.api.common;

/**
 * Created by chenli on 3/31/16.
 */
public interface IPredicate {

    boolean satisfy(ITuple tuple);
}
