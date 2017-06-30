package edu.uci.ics.textdb.exp.nltk;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.uci.ics.textdb.api.constants.DataConstants.TextdbProject;
import edu.uci.ics.textdb.api.utils.Utils;
import edu.uci.ics.textdb.exp.common.PredicateBase;

public class NltkPredicate extends PredicateBase {
    
    
    @JsonCreator
    public NltkPredicate() {};

    @Override
    public Nltk newOperator() {
        
        return new Nltk();
    }

}
