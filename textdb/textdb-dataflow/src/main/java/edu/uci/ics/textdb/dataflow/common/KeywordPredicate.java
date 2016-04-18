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
public class KeywordPredicate implements IPredicate{

    private final String fieldName;
    private final String keyword;

    public KeywordPredicate(String keyword, String fieldName){
        this.keyword = keyword;
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
            if(fieldValue != null && fieldValue.contains(keyword)){
                return true;
            }
        }
        return false;
    }

}
