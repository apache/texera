package edu.uci.ics.texera.workflow.operators.cloudmapper;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ReferenceGenomeEnum {
    HUMAN_GRCh38("GRCh38"),

    MOUSE_GRCm39("GRCm39"),

    MOUSE_mm10("mm10"),

    HUMAN_hg19("hg19"),

    OTHERS("others");

    private final String name;

    ReferenceGenomeEnum(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }
}