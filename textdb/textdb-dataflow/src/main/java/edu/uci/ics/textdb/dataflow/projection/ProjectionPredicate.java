package edu.uci.ics.textdb.dataflow.projection;

import java.util.List;
import java.util.stream.Collectors;

import edu.uci.ics.textdb.api.dataflow.IPredicate;

public class ProjectionPredicate implements IPredicate {
    
    private List<String> projectionFields;
    
    public ProjectionPredicate(List<String> projectionFields) {
        this.projectionFields = projectionFields.stream().map(s -> s.toLowerCase()).collect(Collectors.toList());
    }
    
    public List<String> getProjectionFields() {
        return projectionFields;
    }
}
