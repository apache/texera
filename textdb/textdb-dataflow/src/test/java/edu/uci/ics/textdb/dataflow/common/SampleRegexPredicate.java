package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.StringField;

/**
 * Created by chenli on 3/31/16.
 */
public class SampleRegexPredicate implements IPredicate {


    private final String fieldName;
    private final String regex;

    public SampleRegexPredicate(String regex, String fieldName){
        this.regex = regex;
        this.fieldName = fieldName;
    }

    @Override
    public boolean satisfy(ITuple tuple) {
        //TODO use Java Regex match to verify if the field satisfies this regex
        return true;
    }
}
