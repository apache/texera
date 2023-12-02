package edu.uci.ics.texera.workflow.operators.visualization.linechart;

import com.fasterxml.jackson.annotation.JsonValue;

public enum LineMode {
    LINES("lines"),
    MARKERS("markers"),
    LINES_MARKERS("lines+markers");
    private final String mode;

    LineMode(String mode) {
        this.mode = mode;
    }

    @JsonValue
    public String getMode() {
        return this.mode;
    }
}