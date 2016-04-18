/**
 * 
 */
package edu.uci.ics.textdb.dataflow.common;

import edu.uci.ics.textdb.api.common.IField;
import edu.uci.ics.textdb.api.common.IPredicate;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.common.field.StringField;

/**
 * @author sandeepreddy602
 *
 */
public class RegexPredicate implements IPredicate{

    private final String fieldName;
    private final String regex;

    public RegexPredicate(String regex, String fieldName){
        this.regex = regex;
        this.fieldName = fieldName;
    }

    @Override
    public boolean satisfy(ITuple tuple) {
        if(tuple == null){
            return false;
        }
        IField field = tuple.getField(fieldName);
        if(field instanceof StringField){
            String fieldValue = ((StringField) field).getValue();
            if(fieldValue != null && fieldValue.matches(regex)){
                return true;
            }
        }
        return false;
    }

}
