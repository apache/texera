package edu.uci.ics.texera.workflow.operators.hashJoin;

import com.fasterxml.jackson.annotation.JsonValue;

public enum JoinType {
    INNER("inner"),
    LEFT_OUTER("left outer"),
    RIGHT_OUTER("right outer"),
    FULL_OUTER("full outer");

    private final String value;

    JoinType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getJoinType() {
        return this.value;
    }

}
