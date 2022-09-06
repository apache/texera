package edu.uci.ics.texera.web.resource.dashboard.workflow;

import com.fasterxml.jackson.annotation.JsonValue;

public enum AccessLevel {
    WRITE("write"),
    READ("read"),
    NONE("none");

    private final String value;
    AccessLevel(String accessLevel) {
        this.value = accessLevel;
    }

    @JsonValue
    public String getAccessLevel() {
        return this.value;
    }
}
