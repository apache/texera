package edu.uci.ics.texera.workflow.operators.projection;

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec;
import edu.uci.ics.texera.workflow.common.tuple.Tuple;
import scala.Function1;
import scala.Serializable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ProjectionOpExec extends MapOpExec {
    private final ProjectionOpDesc opDesc;

    public ProjectionOpExec(ProjectionOpDesc opDesc) {
        this.opDesc = opDesc;
        this.setMapFunc((Function1<Tuple, Tuple> & Serializable) this::processTuple);
    }

    public Tuple processTuple(Tuple t) {
        List<String> attributes = opDesc.attributes;
        List<String> attributesToRemove = t.getSchema().getAttributeNames().stream()
                .filter(item -> !attributes.contains(item))
                .collect(Collectors.toList());

        return Tuple.newBuilder().add(t).removeIfExists(attributesToRemove).build();
    }

}
